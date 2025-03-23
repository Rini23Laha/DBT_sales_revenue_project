SELECT
  campaign_name,
  YEAR(close_date) AS year,
  COUNT(*) AS total_opportunities,
  ROUND(SUM(amount_eur), 2) AS revenue_eur
FROM {{ ref('silver_campaign_data') }}
WHERE stage = 'Closed won'
GROUP BY campaign_name, YEAR(close_date)
ORDER BY year, campaign_name
