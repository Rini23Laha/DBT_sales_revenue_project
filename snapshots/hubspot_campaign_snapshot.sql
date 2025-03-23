{% snapshot hubspot_campaign_snapshot %}

{{
  config(
    target_schema='silver',
    unique_key='hubspot_id',
    strategy='timestamp',
    updated_at='close_date'
  )
}}

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
  salesforce_id,
  current_timestamp() AS close_date  -- Add dummy timestamp if not present
FROM {{ ref('bronze_hubspot_campaign') }}

{% endsnapshot %}
