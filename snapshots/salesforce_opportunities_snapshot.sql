{% snapshot salesforce_opportunities_snapshot %}

{{
  config(
    target_schema='silver',
    unique_key='id',
    strategy='timestamp',
    updated_at='close_date' 
  )
}}

SELECT
  id,
  amount,
  currency,
  close_date,
  stage,
  hubspot_id
FROM {{ ref('bronze_salesforce_opportunities') }}

{% endsnapshot %}
