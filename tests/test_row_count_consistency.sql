SELECT 'Mismatch in row count' AS issue
WHERE (
  (SELECT COUNT(*) FROM {{ ref('silver_campaign_data') }}) <
  (SELECT COUNT(*) FROM {{ ref('gold_revenue_per_campaign') }})
)
