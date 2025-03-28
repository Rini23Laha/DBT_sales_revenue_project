SELECT
  campaign_name,
  ROUND(SUM(amount_eur), 2) AS revenue_eur
FROM {{ ref('silver_campaign_data') }}
WHERE stage = 'Closed won'
GROUP BY campaign_name
