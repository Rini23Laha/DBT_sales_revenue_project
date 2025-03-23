-- tests/test_warn_conversion_rate_bounds.sql

SELECT *
FROM {{ ref('gold_conversion_rate_per_campaign') }}
WHERE conversion_rate_percent < 0 OR conversion_rate_percent > 1
