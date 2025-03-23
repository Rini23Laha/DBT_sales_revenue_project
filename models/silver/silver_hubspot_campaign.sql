SELECT
  id AS hubspot_id,
  email,
  campaign_name,
  campaign_target,
  campaign_category,
  channel,
  city_prospect,
  state_prospect,
  country_prospect,
  salesforce_id
FROM {{ ref('bronze_hubspot_campaign') }}
