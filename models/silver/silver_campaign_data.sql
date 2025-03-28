SELECT DISTINCT
    h.hubspot_id,
    h.email,
    h.campaign_name,
    h.campaign_target,
    h.campaign_category,
    h.channel,
    h.city_prospect,
    h.state_prospect,
    h.country_prospect,

    o.opportunity_id,
    o.amount,
    o.currency,
    o.close_date,
    o.stage,
    o.applied_rate,
    o.amount_eur,
    o.is_active,
    o.dbt_valid_from,
    o.dbt_valid_to,
    o.conversion_rate_to_eur

FROM {{ ref('hubspot_campaign_snapshot') }} h
LEFT JOIN {{ ref('silver_opportunities_with_currency') }} o
  ON h.salesforce_id = o.hubspot_id
WHERE o.is_active = true
