SELECT DISTINCT cod_tema, tema, relevancia from
{{ source('camara_raw', 'proposicoes_temas') }}
WHERE cod_tema IS NOT NULL
  AND tema IS NOT NULL
  AND relevancia IS NOT NULL