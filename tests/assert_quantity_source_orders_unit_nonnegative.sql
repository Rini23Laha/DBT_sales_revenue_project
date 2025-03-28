{{config (severity = 'warn') }}

SELECT *
FROM {{ source('landing', 'orders') }}
WHERE unit_price < 0 OR quantity < 0

