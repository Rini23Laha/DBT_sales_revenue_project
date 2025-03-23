-- Step 1: Get all versions of opportunity snapshot (SCD2)
WITH all_opportunities AS (
    SELECT
        id AS opportunity_id,
        amount,
        currency,
        TO_DATE(close_date) AS close_date,
        stage,
        hubspot_id,
        dbt_valid_from,
        dbt_valid_to
    FROM {{ ref('salesforce_opportunities_snapshot') }}
),

-- Step 2: Flag the latest record for each opportunity (based on dbt_valid_from or close_date)
ranked_opportunities AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY opportunity_id
            ORDER BY dbt_valid_from DESC, close_date DESC
        ) AS rn
    FROM all_opportunities
),

-- Step 3: Add is_active flag (true for latest, false for historical records)
flagged_opportunities AS (
    SELECT *,
        CASE WHEN rn = 1 THEN TRUE ELSE FALSE END AS is_active
    FROM ranked_opportunities
),

-- Step 4: Join with the most recent valid currency conversion rate (based on close_date)
-- Step 4: Join with the most recent valid currency conversion rate (based on close_date)
currency_lookup AS (
    SELECT
        o.*,
        c.conversion_rate_to_eur,
        ROW_NUMBER() OVER (
            PARTITION BY o.opportunity_id, o.dbt_valid_from  -- ✅ Partition by version, not just ID
            ORDER BY c.start_date DESC
        ) AS rate_rank
    FROM flagged_opportunities o
    LEFT JOIN {{ ref('silver_currency_conversion') }} c
      ON o.currency = c.isocode
     AND c.start_date <= o.close_date
)


-- Step 5: Select the most recent currency rate per opportunity
SELECT
    opportunity_id,
    amount,
    currency,
    close_date,
    stage,
    hubspot_id,
    is_active,
    dbt_valid_from,
    COALESCE(dbt_valid_to, TO_DATE('9999-12-31')) AS dbt_valid_to,  -- Infinite end date for current record
    conversion_rate_to_eur,
    COALESCE(conversion_rate_to_eur, 1.0) AS applied_rate,
    amount * COALESCE(conversion_rate_to_eur, 1.0) AS amount_eur
FROM currency_lookup
WHERE rate_rank = 1
