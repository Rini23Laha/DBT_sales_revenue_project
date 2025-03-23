SELECT
  isocode,
  conversion_rate_to_eur,
  TO_DATE(start_date) AS start_date,
  TO_DATE(end_date) AS end_date
FROM {{ ref('bronze_currency_conversion') }}
