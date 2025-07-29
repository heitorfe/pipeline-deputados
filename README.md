# 🏛️ Pipeline de Dados da Câmara dos Deputados

## Visão Geral

Pipeline de dados end-to-end para análise de gastos públicos e atividade parlamentar dos deputados federais brasileiros, utilizando dados abertos da API da Câmara dos Deputados. O projeto implementa uma arquitetura moderna de engenharia de dados com ingestão, transformação e visualização automatizadas.

Link para a fonte dos dados: `https://dadosabertos.camara.leg.br/swagger/api.html?tab=api#staticfile`

---

## 🛠️ Stack Tecnológica

### Ingestão de Dados
- **Python**: Scripts para extração inicial completa dos dados históricos  
- **Apache Airflow**: Orquestração da ingestão incremental diária  
- **Pandas**: Manipulação e transformação dos dados durante a extração  
- **Amazon S3**: Data Lake para armazenamento dos arquivos brutos em formato Parquet  

### Armazenamento e Processamento
- **Snowflake**: Data Warehouse principal com separação de ambientes (`RAW`, `STAGING`, `ANALYTICS`)  
- **Snowpipe**: Ingestão automática e incremental acionada por eventos do S3  
- **AWS IAM**: Configuração de roles e políticas de segurança para integração  

### Transformação de Dados
- **dbt (Data Build Tool)**: Modelagem dimensional e transformações ELT  
- **SQL**: Queries complexas para criação de dimensões e fatos  
- **dbt Seeds**: Carregamento de dados de referência estáticos  

### Visualização e Análise
- **Streamlit**: Dashboard interativo para análise individual de deputados  
- **Plotly**: Gráficos dinâmicos e visualizações avançadas  
- **Snowflake Connector**: Conexão direta com o Data Warehouse  

---

## 🏗️ Arquitetura do Projeto

### 1. Camada de Ingestão
- **Carga Inicial (Full Load)**: Scripts Python que consomem a API da Câmara dos Deputados para extrair dados históricos completos de despesas, deputados, legislaturas e outros datasets  
- **Carga Incremental**: DAG do Airflow executada diariamente que:
  - Identifica deputados com mandato ativo  
  - Extrai despesas dos últimos dois meses  
  - Salva os dados em formato Parquet no S3  

### 2. Camada de Armazenamento
- **S3 Data Lake**: Armazenamento de arquivos brutos organizados por tipo e período  
- **Snowpipe**: Detecta automaticamente novos arquivos no S3 e executa `COPY INTO` para carregar dados na tabela de staging  
- **Streams & Tasks**: Sistema de CDC (Change Data Capture) que detecta novos dados e executa `MERGE` para evitar duplicatas  

### 3. Camada de Transformação (dbt)
- **Staging Models**: Limpeza e padronização dos dados brutos  
- **Dimension Models**: Criação de dimensões com SCD Type 2 para histórico de mudanças  
- **Fact Models**: Tabelas de fatos com métricas e chaves estrangeiras  
- **Mart Models**: Views analíticas para consumo do dashboard  

### 4. Camada de Apresentação
- **Dashboard Streamlit**: Interface interativa com:
  - Análise temporal de gastos  
  - Ranking por tipo de despesa  
  - Histórico político dos deputados  
  - Filtros dinâmicos por período e partido  

---

## 📊 Funcionalidades Implementadas

### Pipeline de Dados
- Ingestão automatizada de mais de 2 milhões de registros de despesas  
- Processamento incremental que evita reprocessamento desnecessário  
- Monitoramento e alertas através do Airflow UI  
- Qualidade de dados garantida por testes automatizados do dbt  

### Modelagem Dimensional
- **Dimensão Tempo**: Gerada automaticamente com hierarquias (ano, mês, trimestre)  
- **Dimensão Deputados**: SCD Type 2 para rastrear mudanças de partido/estado  
- **Dimensão Fornecedores**: Normalização de dados de prestadores de serviços  
- **Fato Despesas**: Tabela central com todas as métricas financeiras  

### Dashboard Analítico
- KPIs executivos (total gasto, número de despesas, fornecedores únicos)  
- Análises temporais com contexto histórico  
- Drill-down por tipo de despesa e fornecedor  
- Exportação de dados detalhados  

---

## 🎯 Resultados e Impacto
- **Volume de Dados**: Processamento de ~2M registros de despesas desde 2019  
- **Performance**: Queries analíticas executadas em <2 segundos  
- **Automação**: Pipeline 100% automatizado com execução diária  
- **Qualidade**: Cobertura de testes de qualidade de dados >90%  
- **Escalabilidade**: Arquitetura preparada para crescimento de volume e complexidade  

---

## 🔧 Configurações Técnicas

### Integração Cloud
- Storage Integration entre Snowflake e S3  
- Event notifications do S3 para acionamento do Snowpipe  
- Configuração de roles IAM para acesso seguro entre serviços  

### Orquestração
- DAGs parametrizáveis para diferentes períodos de extração  
- Retry automático em caso de falhas na API  
- Logging detalhado para debugging e monitoramento  

### Qualidade de Dados
- Testes de integridade referencial  
- Validação de tipos e formatos  
- Monitoramento de freshness dos dados  
- Testes de *business rules* específicos do domínio  

---

## 💡 Diferenciais Técnicos
- **Arquitetura Event-Driven**: Uso do Snowpipe para ingestão reativa aos eventos do S3  
- **Processamento Incremental Inteligente**: Evita reprocessamento desnecessário através de controle de datas  
- **Modelagem Avançada**: Implementação de SCD Type 2 para rastreamento histórico  
- **Observabilidade**: Monitoramento completo através de logs estruturados e métricas  

---

Este projeto demonstra domínio completo do ciclo de vida de dados, desde a extração até a apresentação, utilizando as melhores práticas de engenharia de dados e ferramentas modernas do mercado.
