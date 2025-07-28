{{ config(
    materialized='table',
    indexes=[
        {'columns': ['deputado_id'], 'type': 'btree'},
        {'columns': ['ano', 'mes'], 'type': 'btree'},
        {'columns': ['sigla_partido', 'ano'], 'type': 'btree'},
        {'columns': ['sigla_uf', 'ano'], 'type': 'btree'}
    ]
) }}

WITH base_deputados AS (
    SELECT 
        dd.nk_deputado AS deputado_id,
        dd.nome_deputado,
        dd.nome_eleitoral,
        dd.sigla_partido,
        dd.sigla_uf,
        dd.id_legislatura,
        dd.condicao_eleitoral,
        dd.situacao,
        dd.data_inicio_vigencia,
        dd.data_fim_vigencia,
        dd.is_current,
        -- Dados pessoais
        dd.cpf,
        dd.nome_civil,
        dd.data_nascimento,
        dd.municipio_nascimento,
        dd.uf_nascimento,
        dd.sexo
    FROM {{ ref('dim_deputados') }} dd
),

despesas_agregadas AS (
    SELECT 
        deputado_id,
        ano,
        mes,
        COUNT(*) AS qtd_despesas,
        SUM(valor_documento) AS total_valor_documento,
        SUM(valor_liquido) AS total_valor_liquido,
        SUM(valor_glosa) AS total_valor_glosa,
        AVG(valor_liquido) AS media_valor_liquido,
        COUNT(DISTINCT nome_fornecedor) AS qtd_fornecedores_distintos,
        COUNT(DISTINCT tipo_despesa) AS qtd_tipos_despesa_distintos
    FROM {{ ref('stg_despesas') }}
    GROUP BY deputado_id, ano, mes
),

votos_agregados AS (
    SELECT 
        v.deputado_id,
        v.ano,
        v.mes,
        COUNT(*) AS qtd_votos_total,
        SUM(CASE WHEN v.tipo_voto IN ('SIM', 'Sim') THEN 1 ELSE 0 END) AS qtd_votos_sim,
        SUM(CASE WHEN v.tipo_voto IN ('NÃO', 'NAO', 'Não') THEN 1 ELSE 0 END) AS qtd_votos_nao,
        SUM(CASE WHEN v.tipo_voto IN ('Abstenção', 'ABSTENÇÃO', 'ABSTENCAO') THEN 1 ELSE 0 END) AS qtd_votos_abstencao,
        SUM(CASE WHEN v.tipo_voto IN ('Obstrução', 'OBSTRUÇÃO', 'OBSTRUCAO') THEN 1 ELSE 0 END) AS qtd_votos_obstrucao,
        SUM(CASE WHEN v.tipo_voto IN ('ARTIGO 17') THEN 1 ELSE 0 END) AS qtd_votos_artigo_17,
        COUNT(DISTINCT v.id_votacao) AS qtd_votacoes_participou,
        ROUND(
            SUM(CASE WHEN v.tipo_voto IN ('SIM') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
            2
        ) AS percentual_votos_sim,
        ROUND(
            SUM(CASE WHEN v.tipo_voto IN ('NÃO', 'NAO') THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
            2
        ) AS percentual_votos_nao
    FROM {{ ref('stg_votos') }} v
    GROUP BY v.deputado_id, v.ano, v.mes
),

votacoes_do_mes AS (
    SELECT 
        ano,
        mes,
        COUNT(*) AS qtd_votacoes_mes,
        COUNT(DISTINCT proposicao_sigla_tipo) AS qtd_tipos_proposicao,
        COUNT(DISTINCT proposicao_id) AS qtd_proposicoes_votadas
    FROM {{ ref('stg_votacoes') }}
    GROUP BY ano, mes
),

calendario_base AS (
    SELECT DISTINCT ano, mes 
    FROM {{ ref('stg_despesas') }}
    UNION 
    SELECT DISTINCT ano, mes 
    FROM {{ ref('stg_votos') }}
),

tramitacoes_com_lag AS (
    SELECT 
        t.*,
        LAG(t.data_tramitacao) OVER (PARTITION BY t.proposicao_id ORDER BY t.sequencia) AS data_tramitacao_anterior,
        CASE WHEN LAG(t.data_tramitacao) OVER (PARTITION BY t.proposicao_id ORDER BY t.sequencia) IS NOT NULL
        THEN DATEDIFF('day', 
            LAG(t.data_tramitacao) OVER (PARTITION BY t.proposicao_id ORDER BY t.sequencia), 
            t.data_tramitacao)
        END AS dias_entre_tramitacoes
    FROM {{ ref('stg_proposicoes_tramitacoes') }} t
),

tramitacoes_agregadas AS (
    SELECT 
        ano,
        mes,
        COUNT(*) AS qtd_tramitacoes_mes,
        COUNT(DISTINCT proposicao_id) AS qtd_proposicoes_tramitadas,
        COUNT(DISTINCT sigla_orgao) AS qtd_orgaos_tramitaram,
        -- Métricas de atividade legislativa por tipo
        SUM(CASE WHEN UPPER(descricao_tramitacao) LIKE '%APROVAD%' THEN 1 ELSE 0 END) AS tramitacoes_aprovadas,
        SUM(CASE WHEN UPPER(descricao_tramitacao) LIKE '%REJEITAD%' THEN 1 ELSE 0 END) AS tramitacoes_rejeitadas,
        SUM(CASE WHEN UPPER(descricao_tramitacao) LIKE '%ARQUIVAD%' THEN 1 ELSE 0 END) AS tramitacoes_arquivadas,
        -- Análise de velocidade legislativa
        AVG(dias_entre_tramitacoes) AS media_dias_entre_tramitacoes
    FROM tramitacoes_com_lag
    GROUP BY ano, mes
),

-- Relacionamento votações com tramitações para enriquecer contexto
votacoes_com_contexto AS (
    SELECT 
        vot.ano,
        vot.mes,
        vot.qtd_votacoes_mes,
        vot.qtd_tipos_proposicao,
        vot.qtd_proposicoes_votadas,
        -- Adicionar métricas de tramitações do mesmo período
        COALESCE(tra.qtd_tramitacoes_mes, 0) AS qtd_tramitacoes_contexto,
        COALESCE(tra.qtd_proposicoes_tramitadas, 0) AS qtd_proposicoes_tramitadas_contexto
    FROM votacoes_do_mes vot
    LEFT JOIN tramitacoes_agregadas tra ON vot.ano = tra.ano AND vot.mes = tra.mes
),

obt_final AS (
    SELECT 
        -- Chaves e identificadores
        bd.deputado_id,
        cb.ano,
        cb.mes,
        CASE 
            WHEN cb.mes IN (1,2,3) THEN 1
            WHEN cb.mes IN (4,5,6) THEN 2  
            WHEN cb.mes IN (7,8,9) THEN 3
            WHEN cb.mes IN (10,11,12) THEN 4
        END AS trimestre,
        
        -- Dados do deputado
        bd.nome_deputado,
        bd.nome_eleitoral,
        bd.sigla_partido,
        bd.sigla_uf,
        bd.id_legislatura,
        bd.condicao_eleitoral,
        bd.situacao,
        bd.cpf,
        bd.nome_civil,
        bd.data_nascimento,
        bd.municipio_nascimento,
        bd.uf_nascimento,
        bd.sexo,
        bd.is_current,
        
        -- Métricas de despesas
        COALESCE(da.qtd_despesas, 0) AS qtd_despesas,
        COALESCE(da.total_valor_documento, 0) AS total_valor_documento,
        COALESCE(da.total_valor_liquido, 0) AS total_valor_liquido,
        COALESCE(da.total_valor_glosa, 0) AS total_valor_glosa,
        COALESCE(da.media_valor_liquido, 0) AS media_valor_liquido,
        COALESCE(da.qtd_fornecedores_distintos, 0) AS qtd_fornecedores_distintos,
        COALESCE(da.qtd_tipos_despesa_distintos, 0) AS qtd_tipos_despesa_distintos,
        
        -- Métricas de votos
        COALESCE(va.qtd_votos_total, 0) AS qtd_votos_total,
        COALESCE(va.qtd_votos_sim, 0) AS qtd_votos_sim,
        COALESCE(va.qtd_votos_nao, 0) AS qtd_votos_nao,
        COALESCE(va.qtd_votos_abstencao, 0) AS qtd_votos_abstencao,
        COALESCE(va.qtd_votos_obstrucao, 0) AS qtd_votos_obstrucao,
        COALESCE(va.qtd_votacoes_participou, 0) AS qtd_votacoes_participou,
        COALESCE(va.percentual_votos_sim, 0) AS percentual_votos_sim,
        COALESCE(va.percentual_votos_nao, 0) AS percentual_votos_nao,
        
        -- Contexto das votações e tramitações do mês
        COALESCE(vmc.qtd_votacoes_mes, 0) AS qtd_votacoes_mes,
        COALESCE(vmc.qtd_tipos_proposicao, 0) AS qtd_tipos_proposicao,
        COALESCE(vmc.qtd_proposicoes_votadas, 0) AS qtd_proposicoes_votadas,
        COALESCE(vmc.qtd_tramitacoes_contexto, 0) AS qtd_tramitacoes_mes,
        COALESCE(vmc.qtd_proposicoes_tramitadas_contexto, 0) AS qtd_proposicoes_tramitadas_mes,
        
        -- Métricas específicas de tramitações
        COALESCE(ta.tramitacoes_aprovadas, 0) AS tramitacoes_aprovadas_mes,
        COALESCE(ta.tramitacoes_rejeitadas, 0) AS tramitacoes_rejeitadas_mes,
        COALESCE(ta.tramitacoes_arquivadas, 0) AS tramitacoes_arquivadas_mes,
        COALESCE(ta.media_dias_entre_tramitacoes, 0) AS media_dias_entre_tramitacoes,
        
        -- Indicadores de atividade legislativa
        CASE 
            WHEN ta.qtd_tramitacoes_mes > 0 THEN TRUE 
            ELSE FALSE 
        END AS teve_atividade_legislativa,
        
        -- Índice de eficiência legislativa (aprovadas vs total)
        CASE 
            WHEN ta.qtd_tramitacoes_mes > 0 
            THEN ROUND((ta.tramitacoes_aprovadas * 100.0) / ta.qtd_tramitacoes_mes, 2)
            ELSE 0 
        END AS indice_eficiencia_legislativa,
        
        -- Cálculos de participação
        CASE 
            WHEN vmc.qtd_votacoes_mes > 0 
            THEN ROUND((va.qtd_votacoes_participou * 100.0) / vmc.qtd_votacoes_mes, 2)
            ELSE 0 
        END AS percentual_participacao_votacoes,
        
        -- Flags de atividade
        CASE WHEN da.qtd_despesas > 0 THEN TRUE ELSE FALSE END AS teve_despesas,
        CASE WHEN va.qtd_votos_total > 0 THEN TRUE ELSE FALSE END AS participou_votacoes,
        
        CURRENT_TIMESTAMP() AS data_carga
    FROM calendario_base cb
    CROSS JOIN base_deputados bd
    LEFT JOIN despesas_agregadas da 
        ON bd.deputado_id = da.deputado_id 
        AND cb.ano = da.ano 
        AND cb.mes = da.mes
    LEFT JOIN votos_agregados va 
        ON bd.deputado_id = va.deputado_id 
        AND cb.ano = va.ano 
        AND cb.mes = va.mes
    LEFT JOIN votacoes_com_contexto vmc 
        ON cb.ano = vmc.ano 
        AND cb.mes = vmc.mes
    LEFT JOIN tramitacoes_agregadas ta 
        ON cb.ano = ta.ano 
        AND cb.mes = ta.mes
    WHERE 
        -- Filtrar apenas períodos válidos para o deputado
        DATE_FROM_PARTS(cb.ano, cb.mes, 1) >= bd.data_inicio_vigencia
        AND DATE_FROM_PARTS(cb.ano, cb.mes, 1) < bd.data_fim_vigencia
)

SELECT * FROM obt_final
