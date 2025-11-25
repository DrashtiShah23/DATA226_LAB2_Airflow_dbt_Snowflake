SELECT
    SYMBOL,
    DT,
    OPEN,
    HIGH,
    LOW,
    CLOSE,
    VOLUME
FROM {{ source('raw', 'NVDA') }}
WHERE DT IS NOT NULL
  AND CLOSE IS NOT NULL