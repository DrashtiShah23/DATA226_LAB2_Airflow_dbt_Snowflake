-- Using ref() for dbt models - this is correct and creates lineage
WITH all_stocks AS (
    SELECT * FROM {{ ref('stg_aapl') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_nvda') }}
)

SELECT
    SYMBOL,
    DT,
    OPEN,
    HIGH,
    LOW,
    CLOSE,
    VOLUME,
    (HIGH - LOW) AS DAILY_RANGE,
    ((CLOSE - OPEN) / OPEN) * 100 AS DAILY_RETURN_PCT,
    AVG(CLOSE) OVER (
        PARTITION BY SYMBOL 
        ORDER BY DT 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS MA_7_DAY,
    AVG(CLOSE) OVER (
        PARTITION BY SYMBOL 
        ORDER BY DT 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS MA_30_DAY
FROM all_stocks
WHERE DT IS NOT NULL
  AND CLOSE IS NOT NULL