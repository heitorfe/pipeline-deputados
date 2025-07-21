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
        deputados_df['NOME_DEPUTADO'].tolist(),
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

# Dados trimestrais
temporal_query = f"""
SELECT 
    ano,
    trimestre,
    nome_mes,
    SUM(total_valor_liquido) AS gasto_mensal,
    SUM(qtd_despesas) AS despesas_mes
FROM vw_despesas_deputado
WHERE nome_deputado = '{deputado_selecionado}'
GROUP BY ano, trimestre, nome_mes
ORDER BY ano, trimestre
"""
temporal_df = run_query(temporal_query)

# Gráfico combinado - Trimestral e Mensal
col1, col2 = st.columns(2)

with col1:
    # Agregação trimestral
    trimestre_df = temporal_df.groupby(['ANO', 'TRIMESTRE']).agg({
        'GASTO_MENSAL': 'sum',
        'DESPESAS_MES': 'sum'
    }).reset_index()
    
    fig_trimestre = px.bar(
        trimestre_df, 
        x='ANO', 
        y='GASTO_MENSAL', 
        color='TRIMESTRE',
        title="Gastos por Trimestre/Ano",
        labels={'GASTO_MENSAL': 'Valor (R$)', 'ANO': 'Ano'}
    )
    st.plotly_chart(fig_trimestre, use_container_width=True)

with col2:
    # Evolução mensal
    fig_mensal = px.line(
        temporal_df, 
        x='NOME_MES', 
        y='GASTO_MENSAL',
        color='ANO',
        title="Evolução Mensal dos Gastos",
        markers=True
    )
    fig_mensal.update_xaxes(tickangle=45)
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
ORDER BY total_gasto DESC
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
    # Evolução por tipo ao longo dos anos
    fig_evolucao_tipo = px.line(
        tipo_despesa_df, 
        x='ANO', 
        y='TOTAL_GASTO',
        color='TIPO_DESPESA',
        title="Evolução por Tipo de Despesa",
        markers=True
    )
    st.plotly_chart(fig_evolucao_tipo, use_container_width=True)

# ===========================
# Análise por Fornecedor
# ===========================
st.markdown("### 🏪 Análise por Fornecedor")

fornecedor_query = f"""
SELECT 
    df.nome_fornecedor,
    df.nk_fornecedor AS cnpj,
    dt.ano,
    SUM(fd.valor_liquido) AS total_gasto,
    COUNT(fd.cod_documento) AS qtd_transacoes,
    AVG(fd.valor_liquido) AS ticket_medio
FROM fct_despesas fd
JOIN dim_deputados dd ON fd.sk_deputado = dd.sk_deputado
JOIN dim_fornecedores df ON fd.sk_fornecedor = df.sk_fornecedor
JOIN dim_tempo dt ON fd.sk_tempo = dt.sk_tempo
WHERE dd.nome_deputado = '{deputado_selecionado}'
GROUP BY df.nome_fornecedor, df.nk_fornecedor, dt.ano
ORDER BY total_gasto DESC
LIMIT 20
"""
fornecedor_df = run_query(fornecedor_query)

if not fornecedor_df.empty:
    col1, col2 = st.columns(2)
    
    with col1:
        # Top fornecedores
        top_fornecedores = fornecedor_df.groupby('NOME_FORNECEDOR')['TOTAL_GASTO'].sum().reset_index()
        top_fornecedores = top_fornecedores.sort_values('TOTAL_GASTO', ascending=True).tail(10)
        
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
        concentracao = fornecedor_df.groupby('NOME_FORNECEDOR')['TOTAL_GASTO'].sum().reset_index()
        concentracao = concentracao.sort_values('TOTAL_GASTO', ascending=False)
        
        # Top 5 + Outros
        if len(concentracao) > 5:
            top5 = concentracao.head(5)
            outros = pd.DataFrame({
                'NOME_FORNECEDOR': ['OUTROS'],
                'TOTAL_GASTO': [concentracao.tail(-5)['TOTAL_GASTO'].sum()]
            })
            concentracao_final = pd.concat([top5, outros])
        else:
            concentracao_final = concentracao
            
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

# Comparar com médias do partido e estado
comparativa_query = f"""
WITH deputado_stats AS (
    SELECT 
        '{deputado_selecionado}' as deputado,
        SUM(total_valor_liquido) / COUNT(DISTINCT ano) AS media_anual_deputado
    FROM vw_despesas_deputado 
    WHERE nome_deputado = '{deputado_selecionado}'
),
partido_stats AS (
    SELECT 
        AVG(total_valor_liquido) / COUNT(DISTINCT ano) AS media_anual_partido
    FROM vw_despesas_deputado vd
    WHERE vd.sigla_partido = '{deputado_info["SIGLA_PARTIDO"]}'
    AND vd.nome_deputado != '{deputado_selecionado}'
),
uf_stats AS (
    SELECT 
        AVG(total_valor_liquido) / COUNT(DISTINCT ano) AS media_anual_uf
    FROM vw_despesas_deputado vd
    WHERE vd.sigla_uf = '{deputado_info["SIGLA_UF"]}'
    AND vd.nome_deputado != '{deputado_selecionado}'
)
SELECT * FROM deputado_stats, partido_stats, uf_stats
"""
comparativa = run_query(comparativa_query).iloc[0]

col1, col2, col3 = st.columns(3)
col1.metric(
    "Média Anual do Deputado", 
    f"R$ {comparativa['MEDIA_ANUAL_DEPUTADO']:,.2f}"
)
col2.metric(
    f"Média do Partido ({deputado_info['SIGLA_PARTIDO']})", 
    f"R$ {comparativa['MEDIA_ANUAL_PARTIDO']:,.2f}",
    f"{((comparativa['MEDIA_ANUAL_DEPUTADO'] / comparativa['MEDIA_ANUAL_PARTIDO'] - 1) * 100):+.1f}%"
)
col3.metric(
    f"Média do Estado ({deputado_info['SIGLA_UF']})", 
    f"R$ {comparativa['MEDIA_ANUAL_UF']:,.2f}",
    f"{((comparativa['MEDIA_ANUAL_DEPUTADO'] / comparativa['MEDIA_ANUAL_UF'] - 1) * 100):+.1f}%"
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
