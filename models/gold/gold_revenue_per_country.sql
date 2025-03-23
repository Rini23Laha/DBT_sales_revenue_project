
SELECT
  country_prospect,
  YEAR(close_date) AS year,
  stage,
  ROUND(SUM(amount_eur), 2) AS revenue_eur
FROM {{ ref('silver_campaign_data') }}
WHERE close_date IS NOT NULL
GROUP BY country_prospect, year, stage
ORDER BY country_prospect, year, stage