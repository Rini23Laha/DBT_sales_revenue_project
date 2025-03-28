SELECT
  id AS opportunity_id,
  amount,
  currency,
  TO_DATE(close_date) AS close_date,
  stage,
  hubspot_id
FROM {{ ref('bronze_salesforce_opportunities') }}
WHERE id IS NOT NULL AND close_date IS NOT NULL
