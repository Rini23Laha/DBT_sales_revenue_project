SELECT *
FROM {{ ref('gold_revenue_per_campaign') }}
WHERE revenue_eur < 0
