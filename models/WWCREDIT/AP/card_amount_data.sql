{{ config(
    schema='conformed',
    materialized='table'
) }}



WITH dis AS (
  SELECT DISTINCT
    merchant_number_guid,
    settled_amount_diff,
    file_date_diff,
    settle_alpha_currency_code_diff,
    corp_guid,
    region_guid,
    SERVICE_LEVEL_CODE_TRAN_EOD,
    tran_code_diff,
    CARD_TYPE_DIFF,
    lotr_val as etlbatchid,
    CONCAT(tran_code_diff, CARD_TYPE_DIFF, SERVICE_LEVEL_CODE_TRAN_EOD) AS lotr_val,
    COALESCE(pos_entry_code_diff, 'NA') AS pos_entry_code_diff,
    COALESCE(moto_ec_indicator_diff, 'NA') AS moto_ec_indicator_diff,
    COALESCE(cardholder_id_method_diff, 'NA') AS cardholder_id_method_diff,
    trans_sk_diff,
    DATE_TRUNC('MONTH', CURRENT_DATE),
    LAST_DAY(CURRENT_DATE)
  FROM {{ source('wwcredit', 'SRC_MERCH_DATA') }}
  WHERE
    file_date_diff >= (
      DATE_TRUNC('MONTH', CURRENT_DATE)
    )
    AND file_date_diff <= (
      LAST_DAY(CURRENT_DATE)
    )    
), dcr AS (
  SELECT DISTINCT
    conversion_rate,
    conversion_date,
    from_currency,
    settle_alpha_currency_code_diff
  FROM {{ source('wwcredit', 'SRC_CURRENCY_DATA') }}
)
SELECT DISTINCT
  merchant_number_guid,
  dcr.settle_alpha_currency_code_diff,
  corp_guid,
  lotr_val,
  COALESCE(settled_amount_diff, 0) * COALESCE(conversion_rate, 0) AS converted_settled_amount_diff,
  tran_code_diff,
  CARD_TYPE_DIFF,
  LPAD(CAST(DATE_PART(MONTH, file_date_diff) AS TEXT), 2, '0') AS month,
  CAST(DATE_PART(YEAR, file_date_diff) AS TEXT) AS year,
  trans_sk_diff,
  conversion_rate,
  pos_entry_code_diff,
  cardholder_id_method_diff,
  TRIM(moto_ec_indicator_diff) AS moto_ec_indicator_diff,
  SERVICE_LEVEL_CODE_TRAN_EOD,
  etlbatchid  
FROM dis AS dis
INNER JOIN dcr AS dcr
  ON dis.file_date_diff = dcr.conversion_date
  AND dis.settle_alpha_currency_code_diff = dcr.from_currency