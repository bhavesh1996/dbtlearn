{{ config(
    materialized='ephemeral'
) }}

WITH temp_data_for_csv AS (
    select * from {{ ref('wwcredit_ap_merchant_data') }} where etlbatchid in (select max(etlbatchid) from {{ ref('wwcredit_ap_merchant_data') }}) 
)