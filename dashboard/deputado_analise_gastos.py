import os
import streamlit as st
import snowflake.connector
from dotenv import load_dotenv
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

# ===========================
# Carregar variáveis do .env
# ===========================
load_dotenv()
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")

# ===========================
# Configuração da Página
# ===========================
st.set_page_config(page_title="Análise Individual de Gastos Parlamentares", layout="wide")

# ===========================
# Conexão Snowflake
# ===========================
@st.cache_resource
def create_connection():
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    return conn

conn = create_connection()

@st.cache_data
def run_query(query):
    cur = conn.cursor()
    cur.execute(query)
    df = cur.fetch_pandas_all()
    cur.close()
    return df

# ===========================
# Filtro Principal - Seleção do Deputado
# ===========================
st.title("🔍 Análise Individual de Gastos Parlamentares")

# Buscar lista de deputados
deputados_query = """
SELECT DISTINCT 
    dd.nome_deputado,
    dd.sigla_partido,
    dd.sigla_uf,
    dd.url_foto,
    dd.nk_deputado
FROM dim_deputados dd
INNER JOIN fct_despesas fd ON dd.sk_deputado = fd.sk_deputado
ORDER BY dd.nome_deputado
"""
deputados_df = run_query(deputados_query)

# Seletor de deputado
col1, col2 = st.columns([3, 1])
with col1:
    deputado_selecionado = st.selectbox(
        "Selecione o Deputado para Análise:",
        deputados_df['NOME_DEPUTADO'].unique().tolist(),
        help="Escolha um deputado para análise detalhada dos gastos"
    )

# Buscar dados do deputado selecionado
deputado_info = deputados_df[deputados_df['NOME_DEPUTADO'] == deputado_selecionado].iloc[0]

# ===========================
# Cabeçalho com Foto do Deputado
# ===========================
st.markdown("---")
col1, col2, col3 = st.columns([1, 2, 1])

with col1:
    if deputado_info['URL_FOTO']:
        st.image(deputado_info['URL_FOTO'], width=200, caption=deputado_selecionado)
    else:
        st.info("📷 Foto não disponível")

with col2:
    st.markdown(f"## {deputado_selecionado}")
    st.markdown(f"**Partido:** {deputado_info['SIGLA_PARTIDO']}")
    st.markdown(f"**Estado:** {deputado_info['SIGLA_UF']}")

# ===========================
# KPIs Gerais do Deputado
# ===========================
kpis_query = f"""
SELECT 
    SUM(vd.total_valor_liquido) AS total_gasto,
    SUM(vd.qtd_despesas) AS total_despesas,
    COUNT(DISTINCT vd.ano) AS anos_atividade,
    AVG(vd.total_valor_liquido) AS media_mensal,
    MIN(vd.ano) AS primeiro_ano,
    MAX(vd.ano) AS ultimo_ano
FROM vw_despesas_deputado vd
WHERE vd.nome_deputado = '{deputado_selecionado}'
"""
kpis = run_query(kpis_query).iloc[0]

st.markdown("### 📊 Resumo Geral")
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("💰 Total Gasto", f"R$ {kpis['TOTAL_GASTO']:,.2f}")
col2.metric("📄 Nº Despesas", f"{kpis['TOTAL_DESPESAS']:,}")
col3.metric("📅 Anos Ativos", f"{kpis['ANOS_ATIVIDADE']}")
col4.metric("📈 Média Mensal", f"R$ {kpis['MEDIA_MENSAL']:,.2f}")
col5.metric("🗓️ Período", f"{kpis['PRIMEIRO_ANO']}-{kpis['ULTIMO_ANO']}")

# ===========================
# Análise Temporal - Trimestral e Anual
# ===========================
st.markdown("### 📈 Evolução Temporal dos Gastos")

# Dados trimestrais - Query corrigida
temporal_query = f"""
SELECT 
    ano,
    trimestre,
    nome_mes,
    CASE 
        WHEN nome_mes = 'Janeiro' THEN 'Jan' WHEN nome_mes = 'Fevereiro' THEN 'Fev' WHEN nome_mes = 'Março' THEN 'Mar'
        WHEN nome_mes = 'Abril' THEN 'Abr' WHEN nome_mes = 'Maio' THEN 'Mai' WHEN nome_mes = 'Junho' THEN 'Jun'
        WHEN nome_mes = 'Julho' THEN 'Jul' WHEN nome_mes = 'Agosto' THEN 'Ago' WHEN nome_mes = 'Setembro' THEN 'Set'
        WHEN nome_mes = 'Outubro' THEN 'Out' WHEN nome_mes = 'Novembro' THEN 'Nov' WHEN nome_mes = 'Dezembro' THEN 'Dez'
    END AS mes_abrev,
    CASE 
        WHEN nome_mes = 'Janeiro' THEN 1 WHEN nome_mes = 'Fevereiro' THEN 2 WHEN nome_mes = 'Março' THEN 3
        WHEN nome_mes = 'Abril' THEN 4 WHEN nome_mes = 'Maio' THEN 5 WHEN nome_mes = 'Junho' THEN 6
        WHEN nome_mes = 'Julho' THEN 7 WHEN nome_mes = 'Agosto' THEN 8 WHEN nome_mes = 'Setembro' THEN 9
        WHEN nome_mes = 'Outubro' THEN 10 WHEN nome_mes = 'Novembro' THEN 11 WHEN nome_mes = 'Dezembro' THEN 12
    END AS mes,
    SUM(total_valor_liquido) AS gasto_mensal,
    SUM(qtd_despesas) AS despesas_mes
FROM vw_despesas_deputado
WHERE nome_deputado = '{deputado_selecionado}'
GROUP BY ano, trimestre, nome_mes
ORDER BY ano, mes
"""
temporal_df = run_query(temporal_query)

# Gráfico combinado - Trimestral e Mensal
col1, col2 = st.columns(2)

with col1:
    # Agregação trimestral com rótulos corretos
    trimestre_df = temporal_df.groupby(['ANO', 'TRIMESTRE']).agg({
        'GASTO_MENSAL': 'sum',
        'DESPESAS_MES': 'sum'
    }).reset_index()
    
    # Criar rótulo ano-trimestre
    trimestre_df['ANO_TRIMESTRE'] = trimestre_df['ANO'].astype(str) + '-T' + trimestre_df['TRIMESTRE'].astype(str)
    
    fig_trimestre = px.bar(
        trimestre_df, 
        x='ANO_TRIMESTRE', 
        y='GASTO_MENSAL',
        title="Gastos por Trimestre",
        labels={'GASTO_MENSAL': 'Valor (R$)', 'ANO_TRIMESTRE': 'Ano-Trimestre'},
        text='GASTO_MENSAL'
    )
    fig_trimestre.update_traces(texttemplate='R$ %{text:,.0f}', textposition='outside')
    fig_trimestre.update_xaxes(tickangle=45)
    st.plotly_chart(fig_trimestre, use_container_width=True)

with col2:
    # Evolução mensal - série temporal contínua
    temporal_df['DATA_PERIODO'] = pd.to_datetime(temporal_df['ANO'].astype(str) + '-' + temporal_df['MES'].astype(str) + '-01')
    temporal_df = temporal_df.sort_values('DATA_PERIODO')
    
    fig_mensal = px.line(
        temporal_df, 
        x='DATA_PERIODO', 
        y='GASTO_MENSAL',
        title="Evolução Mensal dos Gastos",
        markers=True,
        labels={'DATA_PERIODO': 'Período', 'GASTO_MENSAL': 'Valor (R$)'}
    )
    fig_mensal.update_xaxes(tickformat='%b/%Y')
    st.plotly_chart(fig_mensal, use_container_width=True)

# ===========================
# Análise por Tipo de Despesa
# ===========================
st.markdown("### 📂 Análise por Tipo de Despesa")

tipo_despesa_query = f"""
SELECT 
    dtd.tipo_despesa,
    dt.ano,
    SUM(fd.valor_liquido) AS total_gasto,
    COUNT(fd.cod_documento) AS qtd_despesas,
    AVG(fd.valor_liquido) AS valor_medio
FROM fct_despesas fd
JOIN dim_deputados dd ON fd.sk_deputado = dd.sk_deputado
JOIN dim_tipo_despesa dtd ON fd.sk_tipo_despesa = dtd.sk_tipo_despesa
JOIN dim_tempo dt ON fd.sk_tempo = dt.sk_tempo
WHERE dd.nome_deputado = '{deputado_selecionado}'
GROUP BY dtd.tipo_despesa, dt.ano
ORDER BY dt.ano, total_gasto DESC
"""
tipo_despesa_df = run_query(tipo_despesa_query)

col1, col2 = st.columns(2)

with col1:
    # Total por tipo de despesa
    tipo_total = tipo_despesa_df.groupby('TIPO_DESPESA')['TOTAL_GASTO'].sum().reset_index()
    tipo_total = tipo_total.sort_values('TOTAL_GASTO', ascending=True)
    
    fig_tipo = px.bar(
        tipo_total, 
        x='TOTAL_GASTO', 
        y='TIPO_DESPESA',
        orientation='h',
        title="Total por Tipo de Despesa",
        text='TOTAL_GASTO'
    )
    fig_tipo.update_traces(texttemplate='R$ %{text:,.0f}', textposition='outside')
    st.plotly_chart(fig_tipo, use_container_width=True)

with col2:
    # Evolução por tipo ao longo dos anos - eixo x discreto
    fig_evolucao_tipo = px.line(
        tipo_despesa_df, 
        x='ANO', 
        y='TOTAL_GASTO',
        color='TIPO_DESPESA',
        title="Evolução por Tipo de Despesa",
        markers=True,
        labels={'ANO': 'Ano', 'TOTAL_GASTO': 'Valor (R$)'}
    )
    # Forçar eixo x como discreto
    fig_evolucao_tipo.update_xaxes(type='category', tickmode='linear')
    st.plotly_chart(fig_evolucao_tipo, use_container_width=True)

# ===========================
# Análise por Fornecedor
# ===========================
st.markdown("### 🏪 Análise por Fornecedor")

# Query corrigida para fornecedores
fornecedor_query = f"""
SELECT 
    df.nome_fornecedor,
    df.nk_fornecedor AS cnpj,
    SUM(fd.valor_liquido) AS total_gasto,
    COUNT(fd.cod_documento) AS qtd_transacoes,
    AVG(fd.valor_liquido) AS ticket_medio,
    MIN(dt.ano) AS primeiro_ano,
    MAX(dt.ano) AS ultimo_ano
FROM fct_despesas fd
JOIN dim_deputados dd ON fd.sk_deputado = dd.sk_deputado
JOIN dim_fornecedores df ON fd.sk_fornecedor = df.sk_fornecedor
JOIN dim_tempo dt ON fd.sk_tempo = dt.sk_tempo
WHERE dd.nome_deputado = '{deputado_selecionado}'
GROUP BY df.nome_fornecedor, df.nk_fornecedor
ORDER BY total_gasto DESC
LIMIT 20
"""
fornecedor_df = run_query(fornecedor_query)

if not fornecedor_df.empty:
    col1, col2 = st.columns(2)
    
    with col1:
        # Top fornecedores
        top_fornecedores = fornecedor_df.head(10).copy()
        top_fornecedores = top_fornecedores.sort_values('TOTAL_GASTO', ascending=True)
        
        fig_fornecedor = px.bar(
            top_fornecedores, 
            x='TOTAL_GASTO', 
            y='NOME_FORNECEDOR',
            orientation='h',
            title="Top 10 Fornecedores",
            text='TOTAL_GASTO'
        )
        fig_fornecedor.update_traces(texttemplate='R$ %{text:,.0f}', textposition='outside')
        st.plotly_chart(fig_fornecedor, use_container_width=True)
    
    with col2:
        # Concentração de gastos
        concentracao = fornecedor_df.copy()
        
        # Top 5 + Outros
        if len(concentracao) > 5:
            top5 = concentracao.head(5)
            outros_valor = concentracao.iloc[5:]['TOTAL_GASTO'].sum()
            
            # Criar DataFrame final
            concentracao_final = pd.concat([
                top5[['NOME_FORNECEDOR', 'TOTAL_GASTO']],
                pd.DataFrame({
                    'NOME_FORNECEDOR': ['OUTROS'],
                    'TOTAL_GASTO': [outros_valor]
                })
            ])
        else:
            concentracao_final = concentracao[['NOME_FORNECEDOR', 'TOTAL_GASTO']]
            
        fig_concentracao = px.pie(
            concentracao_final, 
            names='NOME_FORNECEDOR', 
            values='TOTAL_GASTO',
            title="Concentração de Gastos por Fornecedor"
        )
        st.plotly_chart(fig_concentracao, use_container_width=True)

# ===========================
# Análise Comparativa
# ===========================
st.markdown("### 📊 Análise Comparativa")

# Query corrigida para comparações
comparativa_query = f"""
WITH deputado_stats AS (
    SELECT 
        SUM(vd.total_valor_liquido) AS total_deputado,
        COUNT(DISTINCT vd.ano) AS anos_deputado
    FROM vw_despesas_deputado vd
    WHERE vd.nome_deputado = '{deputado_selecionado}'
),
partido_stats AS (
    SELECT 
        AVG(total_anual.total_ano) AS media_anual_partido
    FROM (
        SELECT 
            vd.nome_deputado,
            vd.ano,
            SUM(vd.total_valor_liquido) AS total_ano
        FROM vw_despesas_deputado vd
        WHERE vd.sigla_partido = '{deputado_info["SIGLA_PARTIDO"]}'
        AND vd.nome_deputado != '{deputado_selecionado}'
        GROUP BY vd.nome_deputado, vd.ano
    ) total_anual
),
uf_stats AS (
    SELECT 
        AVG(total_anual.total_ano) AS media_anual_uf
    FROM (
        SELECT 
            vd.nome_deputado,
            vd.ano,
            SUM(vd.total_valor_liquido) AS total_ano
        FROM vw_despesas_deputado vd
        WHERE vd.sigla_uf = '{deputado_info["SIGLA_UF"]}'
        AND vd.nome_deputado != '{deputado_selecionado}'
        GROUP BY vd.nome_deputado, vd.ano
    ) total_anual
)
SELECT 
    ds.total_deputado / ds.anos_deputado AS media_anual_deputado,
    ps.media_anual_partido,
    us.media_anual_uf
FROM deputado_stats ds, partido_stats ps, uf_stats us
"""
comparativa = run_query(comparativa_query).iloc[0]

col1, col2, col3 = st.columns(3)
col1.metric(
    "Média Anual do Deputado", 
    f"R$ {comparativa['MEDIA_ANUAL_DEPUTADO']:,.2f}"
)

# Verificar se há dados para comparação
partido_diff = 0
if pd.notna(comparativa['MEDIA_ANUAL_PARTIDO']) and comparativa['MEDIA_ANUAL_PARTIDO'] > 0:
    partido_diff = ((comparativa['MEDIA_ANUAL_DEPUTADO'] / comparativa['MEDIA_ANUAL_PARTIDO'] - 1) * 100)

col2.metric(
    f"Média do Partido ({deputado_info['SIGLA_PARTIDO']})", 
    f"R$ {comparativa['MEDIA_ANUAL_PARTIDO']:,.2f}" if pd.notna(comparativa['MEDIA_ANUAL_PARTIDO']) else "N/A",
    f"{partido_diff:+.1f}%" if partido_diff != 0 else None
)

uf_diff = 0
if pd.notna(comparativa['MEDIA_ANUAL_UF']) and comparativa['MEDIA_ANUAL_UF'] > 0:
    uf_diff = ((comparativa['MEDIA_ANUAL_DEPUTADO'] / comparativa['MEDIA_ANUAL_UF'] - 1) * 100)

col3.metric(
    f"Média do Estado ({deputado_info['SIGLA_UF']})", 
    f"R$ {comparativa['MEDIA_ANUAL_UF']:,.2f}" if pd.notna(comparativa['MEDIA_ANUAL_UF']) else "N/A",
    f"{uf_diff:+.1f}%" if uf_diff != 0 else None
)

# ===========================
# Tabela Detalhada
# ===========================
st.markdown("### 📋 Detalhamento dos Gastos por Ano")

# Filtro de ano para a tabela
anos_deputado = run_query(f"""
    SELECT DISTINCT ano 
    FROM vw_despesas_deputado 
    WHERE nome_deputado = '{deputado_selecionado}' 
    ORDER BY ano DESC
""")['ANO'].tolist()

ano_tabela = st.selectbox("Selecione o ano para detalhamento:", anos_deputado)

# Query para tabela detalhada
detalhes_query = f"""
SELECT 
    dt.ano,
    dt.nome_mes,
    dtd.tipo_despesa,
    df.nome_fornecedor,
    fd.data_documento,
    fd.num_documento,
    fd.tipo_documento,
    fd.valor_documento,
    fd.valor_liquido,
    fd.valor_glosa,
    fd.url_documento
FROM fct_despesas fd
JOIN dim_deputados dd ON fd.sk_deputado = dd.sk_deputado
JOIN dim_tempo dt ON fd.sk_tempo = dt.sk_tempo
LEFT JOIN dim_tipo_despesa dtd ON fd.sk_tipo_despesa = dtd.sk_tipo_despesa
LEFT JOIN dim_fornecedores df ON fd.sk_fornecedor = df.sk_fornecedor
WHERE dd.nome_deputado = '{deputado_selecionado}'
AND dt.ano = {ano_tabela}
ORDER BY dt.ano, dt.mes, fd.data_documento DESC
"""
detalhes_df = run_query(detalhes_query)

# Formatação da tabela
if not detalhes_df.empty:
    # Formatação de valores monetários
    detalhes_df['VALOR_DOCUMENTO'] = detalhes_df['VALOR_DOCUMENTO'].apply(lambda x: f"R$ {x:,.2f}")
    detalhes_df['VALOR_LIQUIDO'] = detalhes_df['VALOR_LIQUIDO'].apply(lambda x: f"R$ {x:,.2f}")
    detalhes_df['VALOR_GLOSA'] = detalhes_df['VALOR_GLOSA'].apply(lambda x: f"R$ {x:,.2f}")
    
    # Renomear colunas
    detalhes_df.columns = [
        'Ano', 'Mês', 'Tipo Despesa', 'Fornecedor', 'Data Doc.',
        'Nº Doc.', 'Tipo Doc.', 'Valor Doc.', 'Valor Líquido', 
        'Valor Glosa', 'URL Documento'
    ]
    
    st.dataframe(detalhes_df, use_container_width=True, height=400)
    
    # Resumo da tabela
    st.markdown(f"**Total de registros:** {len(detalhes_df)}")
    st.markdown(f"**Período:** {ano_tabela}")
else:
    st.info("Nenhum registro encontrado para o ano selecionado.")

# ===========================
# Rodapé
# ===========================
st.markdown("---")
st.markdown("*Dashboard de Análise Individual de Gastos Parlamentares - Dados da Câmara dos Deputados*")
