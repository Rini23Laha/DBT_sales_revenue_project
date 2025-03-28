-- Step 1: Get all versions of opportunity snapshot (SCD2)
WITH all_opportunities AS (
    SELECT
        id AS opportunity_id,
        amount,
        currency,
        TO_DATE(close_date) AS close_date,
        stage,
        hubspot_id,
        dbt_valid_from
    FROM {{ ref('salesforce_opportunities_snapshot') }}
),

-- Step 2: Add the next dbt_valid_from for each opportunity to compute dbt_valid_to
versioned_opportunities AS (
    SELECT
        *,
        LEAD(dbt_valid_from) OVER (
            PARTITION BY opportunity_id
            ORDER BY dbt_valid_from ASC
        ) AS next_valid_from
    FROM all_opportunities
),

-- Step 3: Flag the latest record (active)
ranked_opportunities AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY opportunity_id
            ORDER BY dbt_valid_from DESC
        ) AS rn
    FROM versioned_opportunities
),

-- Step 4: Add is_active flag and compute correct dbt_valid_to
flagged_opportunities AS (
    SELECT *,
        CASE WHEN rn = 1 THEN TRUE ELSE FALSE END AS is_active,
        CASE 
            WHEN rn = 1 THEN TO_DATE('9999-12-31')  -- Active record
            ELSE dbt_valid_from                     -- dbt_valid_to = dbt_valid_from if inactive
        END AS dbt_valid_to
    FROM ranked_opportunities
),

-- Step 5: Join with the most recent valid currency conversion rate
currency_lookup AS (
    SELECT
        o.*,
        c.conversion_rate_to_eur,
        ROW_NUMBER() OVER (
            PARTITION BY o.opportunity_id, o.dbt_valid_from
            ORDER BY c.start_date DESC
        ) AS rate_rank
    FROM flagged_opportunities o
    LEFT JOIN {{ ref('silver_currency_conversion') }} c
      ON o.currency = c.isocode
     AND c.start_date <= o.close_date
)

-- Step 6: Final output with amount in EUR
SELECT
    opportunity_id,
    amount,
    currency,
    close_date,
    stage,
    hubspot_id,
    is_active,
    dbt_valid_from,
    dbt_valid_to,
    conversion_rate_to_eur,
    COALESCE(conversion_rate_to_eur, 1.0) AS applied_rate,
    amount * COALESCE(conversion_rate_to_eur, 1.0) AS amount_eur
FROM currency_lookup
WHERE rate_rank = 1
