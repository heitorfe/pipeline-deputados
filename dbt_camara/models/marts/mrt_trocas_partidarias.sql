{{
  config(
    materialized='table',
    unique_key='sk_trocas_deputado'
  )
}}

-- Mart Trocas Partidárias: Análise de mudanças de partido e fidelidade partidária
WITH historico_deputados AS (
    SELECT 
        sd.deputado_id,
        sd.nome_civil,
        sd.nome_deputado,
        sd.sigla_partido,
        sd.sigla_uf,
        sd.dbt_valid_from AS data_inicio_vigencia,
        sd.dbt_valid_to AS data_fim_vigencia,
          -- Ordena por data para identificar sequência de partidos
        LAG(sd.sigla_partido) OVER (
            PARTITION BY sd.deputado_id 
            ORDER BY sd.dbt_valid_from
        ) AS partido_anterior,
        
        LEAD(sd.sigla_partido) OVER (
            PARTITION BY sd.deputado_id 
            ORDER BY sd.dbt_valid_from
        ) AS proximo_partido,
        
        -- Identifica se houve mudança
        CASE 
            WHEN LAG(sd.sigla_partido) OVER (
                PARTITION BY sd.deputado_id 
                ORDER BY sd.dbt_valid_from
            ) IS NOT NULL 
            AND LAG(sd.sigla_partido) OVER (
                PARTITION BY sd.deputado_id 
                ORDER BY sd.dbt_valid_from
            ) != sd.sigla_partido 
            THEN TRUE 
            ELSE FALSE 
        END AS flag_mudanca_partido,
        
        -- Calcula tempo no partido (em dias)
        CASE 
            WHEN sd.dbt_valid_to IS NOT NULL 
            THEN DATEDIFF('day', sd.dbt_valid_from, sd.dbt_valid_to)
            ELSE DATEDIFF('day', sd.dbt_valid_from, CURRENT_DATE())
        END AS dias_no_partido,
        
        -- Ano da mudança
        EXTRACT(YEAR FROM sd.dbt_valid_from) AS ano_entrada_partido
        
    FROM {{ ref('snapshot_deputados') }} sd
    WHERE sd.dbt_valid_to IS NULL OR sd.dbt_valid_to > CURRENT_TIMESTAMP()
),

trocas_por_deputado AS (
    SELECT 
        hd.deputado_id,
        hd.nome_civil,
        hd.nome_deputado,        -- Partido atual (último registro)
        FIRST_VALUE(hd.sigla_partido) OVER (
            PARTITION BY hd.deputado_id 
            ORDER BY hd.data_inicio_vigencia DESC
        ) AS partido_atual,
        FIRST_VALUE(hd.sigla_uf) OVER (
            PARTITION BY hd.deputado_id 
            ORDER BY hd.data_inicio_vigencia DESC
        ) AS sigla_uf,
        
        -- Contadores
        COUNT(*) AS total_periodos_partidarios,
        SUM(CASE WHEN hd.flag_mudanca_partido THEN 1 ELSE 0 END) AS total_trocas_partido,        -- Análise temporal
        MIN(hd.data_inicio_vigencia) AS primeira_data_mandato,
        MAX(hd.data_inicio_vigencia) AS ultima_mudanca_partido,
        
        -- Tempo médio em cada partido
        ROUND(AVG(hd.dias_no_partido), 0) AS dias_medio_por_partido,
        
        -- Partidos únicos
        COUNT(DISTINCT hd.sigla_partido) AS partidos_distintos,
        
        -- Lista de partidos (histórico)
        LISTAGG(DISTINCT hd.sigla_partido, ' → ') AS historico_partidos,
        
        -- Trocas por ano
        COUNT(CASE WHEN hd.ano_entrada_partido = 2023 AND hd.flag_mudanca_partido THEN 1 END) AS trocas_2023,
        COUNT(CASE WHEN hd.ano_entrada_partido = 2022 AND hd.flag_mudanca_partido THEN 1 END) AS trocas_2022,
        COUNT(CASE WHEN hd.ano_entrada_partido = 2021 AND hd.flag_mudanca_partido THEN 1 END) AS trocas_2021,
        COUNT(CASE WHEN hd.ano_entrada_partido = 2020 AND hd.flag_mudanca_partido THEN 1 END) AS trocas_2020,
        COUNT(CASE WHEN hd.ano_entrada_partido = 2019 AND hd.flag_mudanca_partido THEN 1 END) AS trocas_2019,        -- Tempo no partido atual (dias desde última mudança)
        DATEDIFF('day', MAX(hd.data_inicio_vigencia), CURRENT_DATE()) AS dias_no_partido_atual,
        
        -- Maior tempo consecutivo em um partido
        MAX(hd.dias_no_partido) AS maior_tempo_consecutivo_partido
        
    FROM historico_deputados hd
    GROUP BY 
        hd.deputado_id,
        hd.nome_civil,
        hd.nome_deputado
),

estatisticas_gerais AS (
    SELECT 
        ROUND(AVG(total_trocas_partido), 2) AS media_trocas_geral,
        ROUND(AVG(dias_medio_por_partido), 0) AS media_dias_por_partido_geral,
        COUNT(CASE WHEN total_trocas_partido = 0 THEN 1 END) AS deputados_sem_troca,
        COUNT(CASE WHEN total_trocas_partido >= 3 THEN 1 END) AS deputados_multiplas_trocas,
        COUNT(*) AS total_deputados_analisados
    FROM trocas_por_deputado
),

trocas_por_uf AS (
    SELECT 
        sigla_uf,
        COUNT(*) AS deputados_uf,
        ROUND(AVG(total_trocas_partido), 2) AS media_trocas_uf,
        COUNT(CASE WHEN total_trocas_partido >= 2 THEN 1 END) AS deputados_com_multiplas_trocas_uf
    FROM trocas_por_deputado
    GROUP BY sigla_uf
),

final AS (
    SELECT 
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key([
            'td.deputado_id'
        ]) }} AS sk_trocas_deputado,
        
        -- Informações do deputado
        td.deputado_id,
        td.nome_civil,
        td.nome_deputado,
        td.partido_atual,
        td.sigla_uf,
        
        -- Métricas de troca
        td.total_periodos_partidarios,
        td.total_trocas_partido,
        td.partidos_distintos,
        td.historico_partidos,
        
        -- Análise temporal
        td.primeira_data_mandato,
        td.ultima_mudanca_partido,
        td.dias_no_partido_atual,
        td.dias_medio_por_partido,
        td.maior_tempo_consecutivo_partido,
        
        -- Trocas por ano
        td.trocas_2023,
        td.trocas_2022,
        td.trocas_2021,
        td.trocas_2020,
        td.trocas_2019,
        
        -- Frequência de trocas (trocas por ano de mandato)
        CASE 
            WHEN DATEDIFF('year', td.primeira_data_mandato, CURRENT_DATE()) > 0
            THEN ROUND(td.total_trocas_partido::FLOAT / DATEDIFF('year', td.primeira_data_mandato, CURRENT_DATE()), 2)
            ELSE 0
        END AS trocas_por_ano_mandato,
        
        -- Estabilidade partidária (% tempo no maior período)
        ROUND(td.maior_tempo_consecutivo_partido::FLOAT / NULLIF(DATEDIFF('day', td.primeira_data_mandato, CURRENT_DATE()), 0) * 100, 2) AS percentual_maior_estabilidade,
        
        -- Comparações
        eg.media_trocas_geral,
        uf.media_trocas_uf,
        
        -- Classificações de fidelidade
        CASE 
            WHEN td.total_trocas_partido = 0 THEN 'Fiel ao Partido'
            WHEN td.total_trocas_partido = 1 THEN 'Uma Troca'
            WHEN td.total_trocas_partido = 2 THEN 'Duas Trocas'
            WHEN td.total_trocas_partido BETWEEN 3 AND 4 THEN 'Múltiplas Trocas'
            ELSE 'Muito Instável'
        END AS classificacao_fidelidade,
        
        CASE 
            WHEN td.dias_medio_por_partido >= 1095 THEN 'Muito Estável'  -- 3+ anos
            WHEN td.dias_medio_por_partido >= 730 THEN 'Estável'         -- 2+ anos
            WHEN td.dias_medio_por_partido >= 365 THEN 'Moderado'        -- 1+ ano
            WHEN td.dias_medio_por_partido >= 180 THEN 'Instável'        -- 6+ meses
            ELSE 'Muito Instável'                                        -- <6 meses
        END AS classificacao_estabilidade,
        
        -- Perfil de migração partidária
        CASE 
            WHEN td.total_trocas_partido = 0 THEN 'Sem Migração'
            WHEN td.trocas_2023 + td.trocas_2022 >= td.total_trocas_partido * 0.7 THEN 'Migração Recente'
            WHEN td.trocas_2019 + td.trocas_2020 >= td.total_trocas_partido * 0.7 THEN 'Migração Início Mandato'
            ELSE 'Migração Distribuída'
        END AS perfil_migracao,
        
        -- Tendência atual
        CASE 
            WHEN td.dias_no_partido_atual >= 730 THEN 'Estabilizado'
            WHEN td.dias_no_partido_atual >= 365 THEN 'Em Consolidação'
            WHEN td.dias_no_partido_atual >= 180 THEN 'Período Inicial'
            ELSE 'Muito Recente'
        END AS status_partido_atual,
        
        -- Flags de análise
        CASE WHEN td.total_trocas_partido = 0 THEN TRUE ELSE FALSE END AS flag_deputado_fiel,
        CASE WHEN td.total_trocas_partido >= 3 THEN TRUE ELSE FALSE END AS flag_multiplas_trocas,
        CASE WHEN td.trocas_2023 > 0 THEN TRUE ELSE FALSE END AS flag_troca_recente,
        CASE WHEN td.total_trocas_partido > eg.media_trocas_geral THEN TRUE ELSE FALSE END AS flag_acima_media_geral,
        CASE WHEN td.total_trocas_partido > uf.media_trocas_uf THEN TRUE ELSE FALSE END AS flag_acima_media_uf,
        CASE WHEN td.dias_medio_por_partido < 365 THEN TRUE ELSE FALSE END AS flag_alta_rotatividade,
        
        -- Métricas de contexto
        eg.deputados_sem_troca AS total_deputados_sem_troca,
        eg.deputados_multiplas_trocas AS total_deputados_multiplas_trocas,
        eg.total_deputados_analisados,
        uf.deputados_com_multiplas_trocas_uf,
        
        -- Metadados
        CURRENT_TIMESTAMP() AS data_processamento
        
    FROM trocas_por_deputado td
    CROSS JOIN estatisticas_gerais eg
    LEFT JOIN trocas_por_uf uf ON td.sigla_uf = uf.sigla_uf
)

SELECT * FROM final
ORDER BY total_trocas_partido DESC, dias_medio_por_partido ASC
