--Recria a tabela com o mesmo nome após a inserção, para adcionar o campo de datetime formatado
CREATE OR REPLACE TABLE `emersondai254.dados_black_desert_online.market_br` AS
SELECT 
*, 
TIMESTAMP_SECONDS(lastSoldTime) as lastUpdateDateTime
FROM `emersondai254.dados_black_desert_online.market_br` 
WHERE name = 'Cristal Mágico Ancestral da Chama Carmesim - Poder'
order by id desc;


--RETIRA OS IDS DUPLICADOS E DEIXA APENAS O ID COM ATUALIZAÇÃO MAIS RECENTE
CREATE OR REPLACE TABLE `emersondai254.dados_black_desert_online.market_br_v2` AS
WITH CTE_DEDUP AS
(
SELECT
*,
ROW_NUMBER() OVER(PARTITION BY id ORDER BY TIMESTAMP_SECONDS(lastSoldTime) DESC) AS UNIQUE_ID
FROM `emersondai254.dados_black_desert_online.market_br` 
)

SELECT
*
FROM CTE_DEDUP
WHERE UNIQUE_ID = 1