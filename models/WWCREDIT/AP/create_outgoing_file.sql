--depends: {{ref('wwcredit_ap_merchant_data')}}

{{ config(
    materialized='ephemeral',
) }}

{% do unload_data_to_snowflake () %}