SELECT
    SYMBOL,
    DT,
    OPEN,
    HIGH,
    LOW,
    CLOSE,
    VOLUME
FROM {{ source('raw', 'AAPL') }}
WHERE DT IS NOT NULL
  AND CLOSE IS NOT NULL