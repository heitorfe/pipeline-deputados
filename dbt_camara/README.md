# DBT Câmara - Data Warehouse Project

This dbt project implements a dimensional data warehouse for Brazilian Chamber of Deputies (Câmara dos Deputados) data.

## Project Structure

- **Staging Layer (`staging/`)**: Data cleansing and standardization from raw sources
- **Dimension Layer (`dimensions/`)**: Dimension tables with surrogate keys
- **Fact Layer (`facts/`)**: Fact tables with measures and foreign keys
- **Mart Layer (`marts/`)**: Analytical views for reporting and dashboards

## Models Overview

### Staging Models
- `stg_deputados`: Cleaned deputados data
- `stg_despesas`: Cleaned expenses data

### Dimension Models
- `dim_deputados`: Deputados dimension
- `dim_fornecedores`: Suppliers dimension
- `dim_tempo`: Time dimension
- `dim_tipo_despesa`: Expense type dimension

### Fact Models
- `fct_despesas`: Expenses fact table

### Mart Models
- `vw_despesas_deputado`: Expenses aggregated by deputado
- `vw_ranking_deputados`: Deputados ranking by expenses

## Getting Started

1. Install dependencies:
   ```bash
   dbt deps
   ```

2. Run the models:
   ```bash
   dbt run
   ```

3. Test the models:
   ```bash
   dbt test
   ```

## Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
