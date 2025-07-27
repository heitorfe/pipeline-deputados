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

# Buscar lista de deputados com despesas (incluindo histórico completo)
deputados_query = """
WITH deputados_com_despesas AS (
    SELECT DISTINCT 
        dd.nk_deputado,
        dd.nome_deputado,
        dd.nome_civil,
        dd.cpf,
        -- Pegar informações do status mais recente
        FIRST_VALUE(dd.sigla_partido) OVER (
            PARTITION BY dd.nk_deputado 
            ORDER BY dd.data_inicio_vigencia DESC
        ) AS sigla_partido_atual,
        FIRST_VALUE(dd.sigla_uf) OVER (
            PARTITION BY dd.nk_deputado 
            ORDER BY dd.data_inicio_vigencia DESC
        ) AS sigla_uf_atual,
        FIRST_VALUE(dd.url_foto) OVER (
            PARTITION BY dd.nk_deputado 
            ORDER BY dd.data_inicio_vigencia DESC
        ) AS url_foto_atual,
        FIRST_VALUE(dd.is_current) OVER (
            PARTITION BY dd.nk_deputado 
            ORDER BY dd.data_inicio_vigencia DESC
        ) AS is_current_deputado,
        -- Estatísticas de despesas (histórico completo)
        COUNT(fd.cod_documento) OVER (PARTITION BY dd.nk_deputado) AS total_despesas,
        SUM(fd.valor_liquido) OVER (PARTITION BY dd.nk_deputado) AS total_valor,
        MIN(dt.ano) OVER (PARTITION BY dd.nk_deputado) AS primeiro_ano_despesa,
        MAX(dt.ano) OVER (PARTITION BY dd.nk_deputado) AS ultimo_ano_despesa,
        COUNT(DISTINCT dd.id_legislatura) OVER (PARTITION BY dd.nk_deputado) AS qtd_legislaturas
    FROM dim_deputados dd
    INNER JOIN fct_despesas fd ON dd.sk_deputado = fd.sk_deputado
    INNER JOIN dim_tempo dt ON fd.sk_tempo = dt.sk_tempo
)
SELECT DISTINCT
    nk_deputado,
    nome_deputado,
    nome_civil,
    cpf,
    sigla_partido_atual AS sigla_partido,
    sigla_uf_atual AS sigla_uf,
    url_foto_atual AS url_foto,
    is_current_deputado AS is_current,
    total_despesas,
    total_valor,
    primeiro_ano_despesa,
    ultimo_ano_despesa,
    qtd_legislaturas,
    CASE 
        WHEN is_current_deputado THEN '🟢 Ativo'
        ELSE '⚪ Ex-Deputado'
    END AS status_mandato
FROM deputados_com_despesas
ORDER BY is_current_deputado DESC, nome_deputado
"""
deputados_df = run_query(deputados_query)

# Verificar se há deputados disponíveis
if deputados_df.empty:
    st.error("❌ Nenhum deputado com despesas encontrado no banco de dados.")
    st.stop()

# Seletor de deputado com informações enriquecidas
# Criar opções com informações adicionais
opcoes_deputados = []
for _, row in deputados_df.iterrows():
    info_adicional = f" | {row['STATUS_MANDATO']} | {row['QTD_LEGISLATURAS']} leg. | {row['PRIMEIRO_ANO_DESPESA']}-{row['ULTIMO_ANO_DESPESA']} | R$ {row['TOTAL_VALOR']:,.0f}"
    opcoes_deputados.append(f"{row['NOME_DEPUTADO']}{info_adicional}")

deputado_selecionado_completo = st.selectbox(
    "Selecione o Deputado para Análise:",
    opcoes_deputados,
    help="Formato: Nome | Status | Legislaturas | Período | Valor Total"
)

# Extrair apenas o nome do deputado
deputado_selecionado = deputado_selecionado_completo.split(" | ")[0]

# Buscar dados do deputado selecionado
deputado_info = deputados_df[deputados_df['NOME_DEPUTADO'] == deputado_selecionado].iloc[0]

# Mostrar informações de contexto do deputado selecionado
st.info(
    f"👤 **{deputado_selecionado}** | "
    f"{deputado_info['STATUS_MANDATO']} | "
    f"Legislaturas: {int(deputado_info['QTD_LEGISLATURAS'])} | "
    f"Período: {int(deputado_info['PRIMEIRO_ANO_DESPESA'])}-{int(deputado_info['ULTIMO_ANO_DESPESA'])} | "
    f"Total: R$ {deputado_info['TOTAL_VALOR']:,.2f} | "
    f"Despesas: {deputado_info['TOTAL_DESPESAS']:,}"
)

# ===========================
# Cabeçalho com Foto e Histórico do Deputado
# ===========================
st.markdown("---")

# Buscar histórico completo do deputado
historico_query = f"""
SELECT 
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
    dd.cpf,
    dd.nome_civil,
    dd.data_nascimento,
    dd.municipio_nascimento,
    dd.uf_nascimento,
    dd.sexo,
    dd.url_foto
FROM dim_deputados dd
WHERE dd.nk_deputado = {deputado_info['NK_DEPUTADO']}
ORDER BY dd.data_inicio_vigencia DESC
"""
historico_df = run_query(historico_query)
deputado_atual = historico_df[historico_df['IS_CURRENT'] == True].iloc[0] if any(historico_df['IS_CURRENT']) else historico_df.iloc[0]

col1, col2, col3 = st.columns([1, 2, 1])

with col1:
    if deputado_atual['URL_FOTO']:
        st.image(deputado_atual['URL_FOTO'], width=200, caption=deputado_selecionado)
    else:
        st.info("📷 Foto não disponível")

with col2:
    st.markdown(f"## {deputado_selecionado}")
    st.markdown(f"**Nome Civil:** {deputado_atual['NOME_CIVIL']}")
    st.markdown(f"**Nome Eleitoral:** {deputado_atual['NOME_ELEITORAL']}")
    
    # Status atual
    st.markdown("### 📋 Status Atual")
    col2a, col2b = st.columns(2)
    with col2a:
        st.markdown(f"**Partido:** {deputado_atual['SIGLA_PARTIDO']}")
        st.markdown(f"**Estado:** {deputado_atual['SIGLA_UF']}")
        st.markdown(f"**Legislatura:** {deputado_atual['ID_LEGISLATURA']}")
    with col2b:
        st.markdown(f"**Condição:** {deputado_atual['CONDICAO_ELEITORAL']}")
        st.markdown(f"**Situação:** {deputado_atual['SITUACAO']}")
        if deputado_atual['DATA_NASCIMENTO']:
            idade = (pd.Timestamp.now() - pd.to_datetime(deputado_atual['DATA_NASCIMENTO'])).days // 365
            st.markdown(f"**Idade:** {idade} anos")

with col3:
    # Informações pessoais
    st.markdown("### 👤 Dados Pessoais")
    st.markdown(f"**Nascimento:** {deputado_atual['DATA_NASCIMENTO'].strftime('%d/%m/%Y') if deputado_atual['DATA_NASCIMENTO'] else 'N/A'}")
    st.markdown(f"**Natural de:** {deputado_atual['MUNICIPIO_NASCIMENTO']}/{deputado_atual['UF_NASCIMENTO']}")
    st.markdown(f"**Sexo:** {deputado_atual['SEXO']}")
    
    # Número de mandatos históricos
    qtd_mandatos = len(historico_df)
    st.markdown(f"**Períodos Históricos:** {qtd_mandatos}")


# ===========================
# KPIs Gerais do Deputado (CORRIGIDO - Histórico Completo)
# ===========================
kpis_query = f"""
WITH despesas_completas AS (
    SELECT 
        fd.valor_liquido,
        fd.cod_documento,
        dt.ano,
        dt.mes,
        dd.sigla_partido,
        dd.sigla_uf,
        dd.id_legislatura
    FROM fct_despesas fd
    INNER JOIN dim_deputados dd ON fd.sk_deputado = dd.sk_deputado
    INNER JOIN dim_tempo dt ON fd.sk_tempo = dt.sk_tempo
    WHERE dd.nk_deputado = {deputado_info['NK_DEPUTADO']}
),
kpis_agregados AS (
    SELECT 
        SUM(valor_liquido) AS total_gasto,
        COUNT(cod_documento) AS total_despesas,
        COUNT(DISTINCT CONCAT(ano, '-', mes)) AS meses_atividade,
        AVG(valor_liquido) AS media_por_despesa,
        MIN(ano) AS primeiro_ano,
        MAX(ano) AS ultimo_ano,
        COUNT(DISTINCT sigla_partido) AS qtd_partidos,
        COUNT(DISTINCT sigla_uf) AS qtd_ufs,
        COUNT(DISTINCT id_legislatura) AS qtd_legislaturas
    FROM despesas_completas
)
SELECT 
    total_gasto,
    total_despesas,
    meses_atividade,
    CASE 
        WHEN meses_atividade > 0 THEN total_gasto / meses_atividade
        ELSE 0 
    END AS media_mensal,
    media_por_despesa,
    primeiro_ano,
    ultimo_ano,
    (ultimo_ano - primeiro_ano + 1) AS anos_periodo,
    qtd_partidos,
    qtd_ufs,
    qtd_legislaturas
FROM kpis_agregados
"""
kpis = run_query(kpis_query).iloc[0]

st.markdown("### 📊 Resumo Geral (Histórico Completo)")
col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("💰 Total Gasto", f"R$ {kpis['TOTAL_GASTO']:,.2f}")
col2.metric("📄 Nº Despesas", f"{int(kpis['TOTAL_DESPESAS']):,}")
col3.metric("📅 Meses Ativos", f"{int(kpis['MESES_ATIVIDADE'])}")
col4.metric("📈 Média Mensal", f"R$ {kpis['MEDIA_MENSAL']:,.2f}")
col5.metric("💳 Média/Despesa", f"R$ {kpis['MEDIA_POR_DESPESA']:,.2f}")
col6.metric("🗓️ Período", f"{int(kpis['PRIMEIRO_ANO']):d}-{int(kpis['ULTIMO_ANO']):d}")

# Métricas adicionais se há mudanças históricas
if kpis['QTD_PARTIDOS'] > 1 or kpis['QTD_UFS'] > 1:
    st.markdown("#### 🔄 Mudanças Históricas")
    col1, col2, col3 = st.columns(3)
    col1.metric("🏛️ Legislaturas", f"{int(kpis['QTD_LEGISLATURAS'])}")
    if kpis['QTD_PARTIDOS'] > 1:
        col2.metric("🎯 Partidos", f"{int(kpis['QTD_PARTIDOS'])}")
    if kpis['QTD_UFS'] > 1:
        col3.metric("🗺️ Estados", f"{kpis['QTD_UFS']}")

# ===========================
# Análise Temporal com Contexto Histórico (Corrigida)
# ===========================
st.markdown("### 📈 Evolução Temporal dos Gastos com Contexto Histórico")

# Dados temporais com informações históricas (query corrigida)
temporal_historico_query = f"""
SELECT 
    dt.ano,
    dt.trimestre,
    dt.nome_mes,
    dt.mes,
    dd.sigla_partido,
    dd.sigla_uf,
    dd.id_legislatura,
    CASE 
        WHEN dt.nome_mes = 'Janeiro' THEN 'Jan' WHEN dt.nome_mes = 'Fevereiro' THEN 'Fev' WHEN dt.nome_mes = 'Março' THEN 'Mar'
        WHEN dt.nome_mes = 'Abril' THEN 'Abr' WHEN dt.nome_mes = 'Maio' THEN 'Mai' WHEN dt.nome_mes = 'Junho' THEN 'Jun'
        WHEN dt.nome_mes = 'Julho' THEN 'Jul' WHEN dt.nome_mes = 'Agosto' THEN 'Ago' WHEN dt.nome_mes = 'Setembro' THEN 'Set'
        WHEN dt.nome_mes = 'Outubro' THEN 'Out' WHEN dt.nome_mes = 'Novembro' THEN 'Nov' WHEN dt.nome_mes = 'Dezembro' THEN 'Dez'
    END AS mes_abrev,
    SUM(fd.valor_liquido) AS gasto_mensal,
    COUNT(fd.cod_documento) AS despesas_mes
FROM fct_despesas fd
INNER JOIN dim_deputados dd ON fd.sk_deputado = dd.sk_deputado
INNER JOIN dim_tempo dt ON fd.sk_tempo = dt.sk_tempo
WHERE dd.nk_deputado = {deputado_info['NK_DEPUTADO']}
GROUP BY dt.ano, dt.trimestre, dt.nome_mes, dt.mes, dd.sigla_partido, dd.sigla_uf, dd.id_legislatura
ORDER BY dt.ano, dt.mes
"""
temporal_historico_df = run_query(temporal_historico_query)

# Gráfico de evolução temporal com contexto histórico
fig_evolucao_temporal = make_subplots(
    rows=2, cols=1,
    shared_xaxes=True,
    vertical_spacing=0.1,
    subplot_titles=("Evolução Mensal dos Gastos", "Total Anual de Despesas por Tipo")
)

# 1. Gráfico de linha: evolução mensal dos gastos
fig_evolucao_temporal.add_trace(
    go.Scatter(
        x=temporal_historico_df['MES_ABREV'] + '/' + temporal_historico_df['ANO'].astype(str),
        y=temporal_historico_df['GASTO_MENSAL'],
        mode='lines+markers',
        name='Gasto Mensal',
        line=dict(color='royalblue', width=2),
        marker=dict(size=4)
    ),
    row=1, col=1
)

# 2. Gráfico de barras empilhadas: total anual de despesas por tipo
# Obter dados de despesas anuais por tipo
despesas_anuais_query = f"""
SELECT 
    dt.ano,
    dtd.tipo_despesa,
    SUM(fd.valor_liquido) AS total_anual
FROM fct_despesas fd
JOIN dim_deputados dd ON fd.sk_deputado = dd.sk_deputado
JOIN dim_tipo_despesa dtd ON fd.sk_tipo_despesa = dtd.sk_tipo_despesa
JOIN dim_tempo dt ON fd.sk_tempo = dt.sk_tempo
WHERE dd.nk_deputado = {deputado_info['NK_DEPUTADO']}
GROUP BY dt.ano, dtd.tipo_despesa
ORDER BY dt.ano, total_anual DESC
"""
despesas_anuais_df = run_query(despesas_anuais_query)

# Gráfico de barras empilhadas
fig_evolucao_temporal.add_trace(
    go.Bar(
        x=despesas_anuais_df['ANO'],
        y=despesas_anuais_df['TOTAL_ANUAL'],
        name='Total Anual',
        marker=dict(color='lightgreen')
    ),
    row=2, col=1
)

# Ajustes finais no layout
fig_evolucao_temporal.update_layout(
    title="Evolução Temporal dos Gastos com Contexto Histórico",
    xaxis_title="Período",
    yaxis_title="Valor (R$)",
    barmode='stack',
    height=800
)

# Exibir gráfico
st.plotly_chart(fig_evolucao_temporal, use_container_width=True)


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
# Análise por Fornecedor - Top Gastos
# ===========================
st.markdown("### 🏪 Análise por Fornecedor")

# Filtro temporal
col1, col2 = st.columns(2)
with col1:
    anos_disponveis = run_query(f"""
        SELECT DISTINCT dt.ano 
        FROM fct_despesas fd
        JOIN dim_deputados dd ON fd.sk_deputado = dd.sk_deputado
        JOIN dim_tempo dt ON fd.sk_tempo = dt.sk_tempo
        WHERE dd.nk_deputado = {deputado_info['NK_DEPUTADO']}
        ORDER BY dt.ano DESC
    """)['ANO'].tolist()
    
    filtro_ano = st.selectbox("Filtrar por ano:", ['Todos os anos'] + anos_disponveis)

# Query para fornecedores
filtro_ano_sql = "" if filtro_ano == 'Todos os anos' else f"AND dt.ano = {filtro_ano}"

fornecedor_query = f"""
SELECT 
    df.nome_fornecedor,
    SUM(fd.valor_liquido) AS total_gasto,
    COUNT(fd.cod_documento) AS qtd_transacoes,
    AVG(fd.valor_liquido) AS ticket_medio
FROM fct_despesas fd
JOIN dim_deputados dd ON fd.sk_deputado = dd.sk_deputado
JOIN dim_fornecedores df ON fd.sk_fornecedor = df.sk_fornecedor
JOIN dim_tempo dt ON fd.sk_tempo = dt.sk_tempo
WHERE dd.nk_deputado = {deputado_info['NK_DEPUTADO']}
{filtro_ano_sql}
GROUP BY df.nome_fornecedor
ORDER BY total_gasto DESC
LIMIT 15
"""
fornecedor_df = run_query(fornecedor_query)

if not fornecedor_df.empty:
    col1, col2 = st.columns(2)
    
    with col1:
        # Top 15 fornecedores
        fornecedor_df_sorted = fornecedor_df.sort_values('TOTAL_GASTO', ascending=True)
        
        fig_fornec = px.bar(
            fornecedor_df_sorted, 
            x='TOTAL_GASTO', 
            y='NOME_FORNECEDOR',
            orientation='h',
            title=f"Top 15 Fornecedores{' - ' + str(filtro_ano) if filtro_ano != 'Todos os anos' else ' - Histórico Completo'}",
            text='TOTAL_GASTO',
            labels={'TOTAL_GASTO': 'Valor Total (R$)', 'NOME_FORNECEDOR': 'Fornecedor'}
        )
        fig_fornec.update_traces(texttemplate='R$ %{text:,.0f}', textposition='outside')
        fig_fornec.update_layout(height=600)
        st.plotly_chart(fig_fornec, use_container_width=True)
    
    with col2:
        # Top tipos de despesa
        tipo_despesa_query = f"""
        SELECT 
            dtd.tipo_despesa,
            SUM(fd.valor_liquido) AS total_gasto,
            COUNT(fd.cod_documento) AS qtd_transacoes
        FROM fct_despesas fd
        JOIN dim_deputados dd ON fd.sk_deputado = dd.sk_deputado
        JOIN dim_tipo_despesa dtd ON fd.sk_tipo_despesa = dtd.sk_tipo_despesa
        JOIN dim_tempo dt ON fd.sk_tempo = dt.sk_tempo
        WHERE dd.nk_deputado = {deputado_info['NK_DEPUTADO']}
        {filtro_ano_sql}
        GROUP BY dtd.tipo_despesa
        ORDER BY total_gasto DESC
        """
        tipo_despesa_filtrado_df = run_query(tipo_despesa_query)
        
        if not tipo_despesa_filtrado_df.empty:
            tipo_despesa_sorted = tipo_despesa_filtrado_df.sort_values('TOTAL_GASTO', ascending=True)
            
            fig_tipo_filtrado = px.bar(
                tipo_despesa_sorted, 
                x='TOTAL_GASTO', 
                y='TIPO_DESPESA',
                orientation='h',
                title=f"Gastos por Tipo de Despesa{' - ' + str(filtro_ano) if filtro_ano != 'Todos os anos' else ' - Histórico Completo'}",
                text='TOTAL_GASTO',
                labels={'TOTAL_GASTO': 'Valor Total (R$)', 'TIPO_DESPESA': 'Tipo de Despesa'}
            )
            fig_tipo_filtrado.update_traces(texttemplate='R$ %{text:,.0f}', textposition='outside')
            fig_tipo_filtrado.update_layout(height=600)
            st.plotly_chart(fig_tipo_filtrado, use_container_width=True)


# ===========================
# Tabela Detalhada com Contexto Histórico
# ===========================
st.markdown("### 📋 Detalhamento dos Gastos por Período")

# Filtros para a tabela
col1, col2 = st.columns(2)

with col1:
    anos_deputado = run_query(f"""
        SELECT DISTINCT ano 
        FROM vw_despesas_deputado 
        WHERE nk_deputado = {deputado_info['NK_DEPUTADO']}
        ORDER BY ano DESC
    """)['ANO'].tolist()
    
    ano_tabela = st.selectbox("Selecione o ano para detalhamento:", anos_deputado)

with col2:
    partidos_ano = run_query(f"""
        SELECT DISTINCT sigla_partido 
        FROM vw_despesas_deputado 
        WHERE nk_deputado = {deputado_info['NK_DEPUTADO']}
        AND ano = {ano_tabela}
        ORDER BY sigla_partido
    """)['SIGLA_PARTIDO'].tolist()
    
    if len(partidos_ano) > 1:
        partido_filtro = st.selectbox("Partido no período:", ['Todos'] + partidos_ano)
    else:
        partido_filtro = partidos_ano[0] if partidos_ano else 'Todos'
        st.info(f"Partido no período: {partido_filtro}")

# Query para tabela detalhada com contexto histórico
detalhes_historico_query = f"""
SELECT 
    dt.ano,
    dt.nome_mes,
    dd.sigla_partido,
    dd.sigla_uf,
    dd.id_legislatura,
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
WHERE dd.nk_deputado = {deputado_info['NK_DEPUTADO']}
AND dt.ano = {ano_tabela}
{f"AND dd.sigla_partido = '{partido_filtro}'" if partido_filtro != 'Todos' else ""}
ORDER BY dt.ano, dt.mes, fd.data_documento DESC
"""
detalhes_hist_df = run_query(detalhes_historico_query)

# Mostrar resumo do período
if not detalhes_hist_df.empty:
    resumo_periodo = detalhes_hist_df.groupby(['SIGLA_PARTIDO', 'SIGLA_UF', 'ID_LEGISLATURA']).agg({
        'VALOR_LIQUIDO': 'sum',
        'VALOR_DOCUMENTO': 'count'
    }).reset_index()
    
    st.markdown(f"#### 📊 Resumo do Período {ano_tabela}")
    for _, row in resumo_periodo.iterrows():
        st.markdown(f"**{row['SIGLA_PARTIDO']}/{row['SIGLA_UF']} (Leg. {row['ID_LEGISLATURA']}):** R$ {row['VALOR_LIQUIDO']:,.2f} em {row['VALOR_DOCUMENTO']} despesas")
    
    # Formatação da tabela
    detalhes_hist_df['VALOR_DOCUMENTO'] = detalhes_hist_df['VALOR_DOCUMENTO'].apply(lambda x: f"R$ {x:,.2f}")
    detalhes_hist_df['VALOR_LIQUIDO'] = detalhes_hist_df['VALOR_LIQUIDO'].apply(lambda x: f"R$ {x:,.2f}")
    detalhes_hist_df['VALOR_GLOSA'] = detalhes_hist_df['VALOR_GLOSA'].apply(lambda x: f"R$ {x:,.2f}")
    
    # Renomear colunas
    detalhes_hist_df.columns = [
        'Ano', 'Mês', 'Partido', 'UF', 'Legislatura', 'Tipo Despesa', 'Fornecedor', 
        'Data Doc.', 'Nº Doc.', 'Tipo Doc.', 'Valor Doc.', 'Valor Líquido', 
        'Valor Glosa', 'URL Documento'
    ]
    
    st.dataframe(detalhes_hist_df, use_container_width=True, height=400)
    
    # Resumo da tabela
    st.markdown(f"**Total de registros:** {len(detalhes_hist_df)}")
    st.markdown(f"**Período:** {ano_tabela} - Filtro: {partido_filtro}")
else:
    st.info("Nenhum registro encontrado para o período selecionado.")


# ===========================
# Histórico Político do Deputado
# ===========================
st.markdown("### 🏛️ Histórico Político")

if len(historico_df) > 1:
    # Criar timeline do histórico
    historico_display = historico_df.copy()
    historico_display['ano'] = historico_display['DATA_INICIO_VIGENCIA'].dt.year
    historico_display = historico_display.drop_duplicates(subset=[ 'ano', 'SITUACAO', 'ID_LEGISLATURA'])
    historico_display['PERIODO'] = historico_display.apply(
        lambda x: f"{x['DATA_INICIO_VIGENCIA'].strftime('%m/%Y')} - {'Atual' if x['IS_CURRENT'] else x['DATA_FIM_VIGENCIA'].strftime('%m/%Y')}", 
        axis=1
    )
    
    # Criar timeline horizontal
    timeline_data = []
    for _, row in historico_display.iterrows():
        timeline_data.append({
            'data_inicio': row['DATA_INICIO_VIGENCIA'],
            'data_fim': row['DATA_FIM_VIGENCIA'] if not row['IS_CURRENT'] else pd.Timestamp.now(),
            'periodo': row['PERIODO'],
            'partido': row['SIGLA_PARTIDO'],
            'uf': row['SIGLA_UF'],
            'legislatura': row['ID_LEGISLATURA'],
            'situacao': row['SITUACAO'],
            'is_current': row['IS_CURRENT']
        })
    
    timeline_df = pd.DataFrame(timeline_data)
    
    # Criar gráfico de timeline horizontal
    fig_timeline = go.Figure()
    
    for idx, row in timeline_df.iterrows():
        # Cor baseada no status
        cor = 'green' if row['is_current'] else 'lightblue'
        
        # Adicionar barra horizontal para cada período
        fig_timeline.add_trace(go.Scatter(
            x=[row['data_inicio'], row['data_fim']],
            y=[idx, idx],
            mode='lines+markers',
            line=dict(color=cor, width=8),
            marker=dict(size=10, color=cor),
            name=f"{row['partido']}/{row['uf']} - Leg. {row['legislatura']}",
            hovertemplate=f"<b>{row['periodo']}</b><br>" +
                         f"Partido: {row['partido']}<br>" +
                         f"UF: {row['uf']}<br>" +
                         f"Legislatura: {row['legislatura']}<br>" +
                         f"Situação: {row['situacao']}<extra></extra>",
            showlegend=False
        ))
        
        # Adicionar texto com informações
        fig_timeline.add_annotation(
            x=row['data_inicio'] + (row['data_fim'] - row['data_inicio']) / 2,
            y=idx + 0.15,
            text=f"{row['partido']}/{row['uf']} - Leg.{row['legislatura']}",
            showarrow=False,
            font=dict(size=10, color='black'),
            bgcolor='white',
            bordercolor='gray',
            borderwidth=1
        )
        
        # Adicionar situação abaixo
        fig_timeline.add_annotation(
            x=row['data_inicio'] + (row['data_fim'] - row['data_inicio']) / 2,
            y=idx - 0.15,
            text=row['situacao'],
            showarrow=False,
            font=dict(size=9, color='gray'),
        )
    
    # Configurar layout
    fig_timeline.update_layout(
        title="Timeline Político do Deputado",
        xaxis_title="Período",
        yaxis=dict(
            showticklabels=False,
            showgrid=False,
            zeroline=False
        ),
        height=max(300, len(timeline_df) * 80),
        showlegend=False,
        hovermode='closest'
    )
    
    st.plotly_chart(fig_timeline, use_container_width=True)
else:
    st.info("Deputado possui apenas um período histórico registrado.")

# ===========================
# Rodapé
# ===========================
st.markdown("---")
st.markdown("*Dashboard de Análise Individual de Gastos Parlamentares com Histórico Político - Dados da Câmara dos Deputados*")
