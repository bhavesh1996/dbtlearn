{{ config(
    materialized='incremental',
    unique_key=['merchant_number_guid','consumption_sk'],
    incremental_strategy='merge',
    schema='consumption',
    merge_update_columns=['total_turnover', 'cnp_keyed_turnover', 'refund_amount', 'updated_at']
) }}

WITH source_data AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['merchant_number_guid', 'total_turnover', 'cnp_keyed_turnover', 'refund_amount']) }} AS consumption_sk,
        TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISSFF3') AS etlbatchid,
        current_timestamp() AS current_timestamp,
        CURRENT_DATE AS etlbatchid_date,
        'AP' AS mtd_region,
        'GPN' AS portfolio,
        merchant_number_guid,
        total_turnover,
        cnp_keyed_turnover,
        refund_amount,
        updated_at,
        RANK() OVER (PARTITION BY merchant_number_guid ORDER BY updated_at DESC) AS rank
    FROM {{ ref('tbl_amount_calc') }}

    {% if is_incremental() %}
        WHERE updated_at > (SELECT coalesce(max(updated_at), '1900-01-01') FROM {{ this }})
    {% endif %}
)

SELECT 
    consumption_sk,
    etlbatchid,
    current_timestamp() as current_timestamp,
    etlbatchid_date,
    mtd_region,
    portfolio,
    merchant_number_guid,
    total_turnover,
    cnp_keyed_turnover,
    refund_amount,
    updated_at
FROM source_data
WHERE rank = 1
