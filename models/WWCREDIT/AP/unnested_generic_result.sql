{{ config(
    schema='confirmed',
    materialized='table'
) }}

SELECT DISTINCT
  (
    transaction_code_inclusion.value || card_type_inclusion.value || service_level_code_inclusion.value )
   AS generic_inclusion,
  card_type_inclusion.value as card_type_credit,
  Charge_type_inclusion.value as Charge_type_inclusion,
  Charge_type_exclusion.value as Charge_type_exclusion,
  Pos_Entry_mode_inclusion.value as Pos_Entry_mode_inclusion,
  Pos_Entry_Mode_Exclusion.value as Pos_Entry_Mode_Exclusion,
  moto_ec_indicator_inclusion.value as moto_ec_indicator_inclusion,
  moto_ec_indicator_exclusion.value as moto_ec_indicator_exclusion,
  card_holder_id_inclusion.value as card_holder_id_inclusion,
  card_holder_id_exclusion.value as card_holder_id_exclusion,
  service_level_code_inclusion.value as service_level_code_inclusion,
  service_level_code_exclusion.value as service_level_code_exclusion,
  transaction_code_inclusion.value as transaction_code_inclusion,
  transaction_code_exclusion.value as transaction_code_exclusion,
  card_scheme,
  result
  
FROM {{ source('wwcredit', 'GENERIC_FIELD_VALUE_RESULT') }}, LATERAL FLATTEN(INPUT => card_type_inclusion_array) AS card_type_inclusion, LATERAL FLATTEN(INPUT => card_type_exclusion_array) AS card_type_exclusion, LATERAL FLATTEN(INPUT => Charge_type_inclusion_array) AS Charge_type_inclusion, LATERAL FLATTEN(INPUT => Charge_type_exclusion_array) AS Charge_type_exclusion, LATERAL FLATTEN(INPUT => Pos_Entry_mode_inclusion_array) AS Pos_Entry_mode_inclusion, LATERAL FLATTEN(INPUT => Pos_Entry_Mode_Exclusion_array) AS Pos_Entry_Mode_Exclusion, LATERAL FLATTEN(INPUT => moto_ec_indicator_inclusion_array) AS moto_ec_indicator_inclusion, LATERAL FLATTEN(INPUT => moto_ec_indicator_exclusion_array) AS moto_ec_indicator_exclusion, LATERAL FLATTEN(INPUT => card_holder_id_inclusion_array) AS card_holder_id_inclusion, LATERAL FLATTEN(INPUT => card_holder_id_exclusion_array) AS card_holder_id_exclusion, LATERAL FLATTEN(INPUT => service_level_code_inclusion_array) AS service_level_code_inclusion, LATERAL FLATTEN(INPUT => service_level_code_exclusion_array) AS service_level_code_exclusion, LATERAL FLATTEN(INPUT => transaction_code_inclusion_array) AS transaction_code_inclusion, LATERAL FLATTEN(INPUT => transaction_code_exclusion_array) AS transaction_code_exclusion
WHERE
  application_name = 'creditshield_ap'