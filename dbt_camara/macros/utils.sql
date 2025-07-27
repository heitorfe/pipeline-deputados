-- Macro para padronizar nomes de partidos políticos
{% macro clean_party_name(party_column) %}
  case 
    when upper({{ party_column }}) in ('AVANTE', 'AVANTE-SP') then 'AVANTE'
    when upper({{ party_column }}) in ('CIDADANIA', 'PPS') then 'CIDADANIA'
    when upper({{ party_column }}) in ('DEMOCRATAS', 'DEM', 'PFL') then 'DEMOCRATAS'
    when upper({{ party_column }}) in ('MDB', 'PMDB') then 'MDB'
    when upper({{ party_column }}) in ('PATRIOTA', 'PEN') then 'PATRIOTA'
    when upper({{ party_column }}) in ('PODE', 'PTN') then 'PODE'
    when upper({{ party_column }}) in ('PP', 'PPB') then 'PP'
    when upper({{ party_column }}) in ('PROS', 'PRO') then 'PROS'
    when upper({{ party_column }}) in ('REPUBLICANOS', 'PRB') then 'REPUBLICANOS'
    when upper({{ party_column }}) in ('SOLIDARIEDADE', 'SD') then 'SOLIDARIEDADE'
    when upper({{ party_column }}) in ('UNIÃO', 'DEM-UNIÃO') then 'UNIÃO'
    else upper({{ party_column }})
  end
{% endmacro %}

-- Macro para categorizar tipos de despesa
{% macro categorize_expense_type(expense_type_column) %}
  case 
    when upper({{ expense_type_column }}) like '%COMBUSTÍVEL%' 
      or upper({{ expense_type_column }}) like '%COMBUSTIVEL%' then 'COMBUSTIVEL'
    when upper({{ expense_type_column }}) like '%TELEFONE%' 
      or upper({{ expense_type_column }}) like '%TELEFÔNICO%' then 'TELEFONE'
    when upper({{ expense_type_column }}) like '%HOSPEDAGEM%' then 'HOSPEDAGEM'
    when upper({{ expense_type_column }}) like '%ALIMENTAÇÃO%' 
      or upper({{ expense_type_column }}) like '%ALIMENTACAO%' then 'ALIMENTACAO'
    when upper({{ expense_type_column }}) like '%PASSAGEM%' 
      or upper({{ expense_type_column }}) like '%TRANSPORTE%' then 'TRANSPORTE'
    when upper({{ expense_type_column }}) like '%LOCAÇÃO%' 
      or upper({{ expense_type_column }}) like '%LOCACAO%' 
      or upper({{ expense_type_column }}) like '%ALUGUEL%' then 'LOCACAO'
    when upper({{ expense_type_column }}) like '%CONSULTORIA%' 
      or upper({{ expense_type_column }}) like '%SERVIÇO%' 
      or upper({{ expense_type_column }}) like '%SERVICO%' then 'SERVICOS'
    when upper({{ expense_type_column }}) like '%MATERIAL%' 
      or upper({{ expense_type_column }}) like '%SUPRIMENTO%' then 'MATERIAL'
    when upper({{ expense_type_column }}) like '%DIVULGAÇÃO%' 
      or upper({{ expense_type_column }}) like '%DIVULGACAO%' then 'DIVULGACAO'
    else 'OUTROS'
  end
{% endmacro %}

-- Macro para calcular idade em anos
{% macro calculate_age_years(birth_date_column) %}
  datediff(year, current_date(), cast({{ birth_date_column }} as date))
{% endmacro %}

-- Macro para extrair ID de URI da API
{% macro extract_id_from_uri(uri_column, resource_type='') %}
  {% if resource_type %}
    cast(regexp_extract({{ uri_column }}, r'/{{ resource_type }}/(\d+)$', 1) as integer)
  {% else %}
    cast(regexp_extract({{ uri_column }}, r'/(\d+)$', 1) as integer)
  {% endif %}
{% endmacro %}

-- Macro para padronizar situações de proposições
{% macro standardize_proposition_status(status_column) %}
  case 
    when upper({{ status_column }}) like '%APROVADA%' 
      or upper({{ status_column }}) like '%APROVADO%' then 'APROVADA'
    when upper({{ status_column }}) like '%REJEITADA%' 
      or upper({{ status_column }}) like '%REJEITADO%' then 'REJEITADA'
    when upper({{ status_column }}) like '%ARQUIVADA%' 
      or upper({{ status_column }}) like '%ARQUIVADO%' then 'ARQUIVADA'
    when upper({{ status_column }}) like '%TRAMITAÇÃO%' 
      or upper({{ status_column }}) like '%TRAMITACAO%' then 'EM_TRAMITACAO'
    when upper({{ status_column }}) like '%PAUTA%' then 'PRONTA_PAUTA'
    when upper({{ status_column }}) like '%NORMA%' 
      or upper({{ status_column }}) like '%LEI%' then 'TRANSFORMADA_NORMA'
    else upper({{ status_column }})
  end
{% endmacro %}

-- Macro para formatar valores monetários brasileiros
{% macro format_currency_brl(amount_column) %}
  concat('R$ ', format_number({{ amount_column }}, 2))
{% endmacro %}

-- Macro para gerar hash de auditoria
{% macro generate_audit_hash(columns) %}
  sha256(concat(
    {% for column in columns %}
      coalesce(cast({{ column }} as string), 'NULL')
      {%- if not loop.last -%},{%- endif -%}
    {% endfor %}
  ))
{% endmacro %}
