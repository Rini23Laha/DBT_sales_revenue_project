SELECT
  campaign_name,
  COUNT(*) AS total_opportunities,
  COUNT(CASE WHEN stage = 'Closed won' THEN 1 END) AS won_opportunities,
  ROUND(100.0 * COUNT(CASE WHEN stage = 'Closed won' THEN 1 END) / COUNT(*), 2) AS conversion_rate_percent
FROM {{ ref('silver_campaign_data') }}
GROUP BY campaign_name
