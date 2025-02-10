{{ config(
    schema='confirmed',
    materialized='table'
) }}

WITH dis AS (
  SELECT DISTINCT
    merchant_number_guid,
    month,
    year,
    settle_alpha_currency_code_diff,
    corp_guid,
    CARD_TYPE_DIFF,
    lotr_val,
    trans_sk_diff,
    CONCAT(tran_code_diff, CARD_TYPE_DIFF) AS lotr_tran_card,
    converted_settled_amount_diff,
    pos_entry_code_diff,
    cardholder_id_method_diff,
    moto_ec_indicator_diff,
    SERVICE_LEVEL_CODE_TRAN_EOD    
  FROM {{ source('wwcredit', 'diff_amount_data') }} AS diff_amount_data
), cte AS (
  SELECT DISTINCT
    generic_inclusion,
    card_type_credit,
    Charge_type_inclusion,
    Charge_type_exclusion,
    Pos_Entry_mode_inclusion,
    Pos_Entry_Mode_Exclusion,
    moto_ec_indicator_inclusion,
    moto_ec_indicator_exclusion,
    card_holder_id_inclusion,
    card_holder_id_exclusion,
    service_level_code_inclusion,
    service_level_code_exclusion,
    transaction_code_inclusion,
    transaction_code_exclusion,
    card_scheme,
    result,
    CONCAT(card_scheme, CONCAT(' ', result), ' CREDIT') AS credit
  FROM {{ ref('unnested_generic_result') }} AS unnested_generic_result
), cte_2 AS (
  SELECT DISTINCT
    generic_inclusion,
    credit,
    card_scheme,
    result,
    Pos_Entry_mode_inclusion,
    card_holder_id_inclusion,
    moto_ec_indicator_inclusion,
    service_level_code_inclusion,
    transaction_code_inclusion,
    card_type_credit,
    CONCAT(transaction_code_inclusion, card_type_credit) AS generic_tran_card,
    CONCAT(transaction_code_exclusion, card_type_credit, service_level_code_inclusion) AS generic_refund
  FROM cte AS cte
), cte_3 AS (
  SELECT DISTINCT
    merchant_number_guid,
    corp_guid,
    converted_settled_amount_diff,
    credit,
    trans_sk_diff,
    month,
    year,
    settle_alpha_currency_code_diff,
    CASE
      WHEN lotr_val = generic_inclusion
      AND (
        pos_entry_code_diff = Pos_Entry_mode_inclusion
        OR cardholder_id_method_diff = card_holder_id_inclusion
        OR moto_ec_indicator_diff = moto_ec_indicator_inclusion
      )
      THEN 6
      ELSE CASE
        WHEN lotr_val = generic_inclusion
        THEN 5
        ELSE CASE
          WHEN lotr_val = generic_refund
          THEN 4
          ELSE CASE
            WHEN lotr_tran_card = generic_tran_card
            THEN 3
            ELSE CASE
              WHEN lotr_tran_card = generic_tran_card
              AND SERVICE_LEVEL_CODE_TRAN_EOD <> service_level_code_inclusion
              THEN 2
              ELSE 1
            END
          END
        END
      END
    END AS tran_code_non_liable_identifier
  FROM dis AS dis
  LEFT JOIN cte_2 AS cte_2
    ON card_type_credit = CARD_TYPE_DIFF
), cte_4 AS (
  SELECT
    merchant_number_guid,
    corp_guid,
    converted_settled_amount_diff,
    credit,
    trans_sk_diff,
    month,
    year,
    settle_alpha_currency_code_diff,
    MAX(tran_code_non_liable_identifier) AS tran_code_non_liable_identifier
  FROM cte_3 AS cte_3
  GROUP BY
    merchant_number_guid,
    corp_guid,
    converted_settled_amount_diff,
    credit,
    trans_sk_diff,
    credit,
    month,
    year,
    settle_alpha_currency_code_diff
), cte_5 AS (
  SELECT
    merchant_number_guid,
    month,
    year,
    settle_alpha_currency_code_diff,
    corp_guid,
    SUM(
      CASE
        WHEN credit IN ('LIABLE CARD TYPES LIABLE LOGIC1 CREDIT', 'LIABLE CARD TYPES LIABLE LOGIC CREDIT')
        AND tran_code_non_liable_identifier IN (6, 5)
        THEN converted_settled_amount_diff
        ELSE 0
      END
    ) AS total_turnover,
    SUM(
      CASE
        WHEN (
          (
            credit IN ('VISA CNP CREDIT', 'VISA CNP DEBIT')
          )
          OR (
            credit IN ('MASTERCARD CNP CREDIT', 'MASTERCARD CNP DEBIT')
          )
          OR (
            credit = 'UNION PAY CNP CREDIT'
          )
          OR (
            credit IN ('AMEX CNP CREDIT', 'JCB CNP CREDIT')
          )
          OR (
            credit IN ('DISCOVER CNP CREDIT', 'PLAM PLPC PRIVATELBL CNP CREDIT', 'INTR MAESTRO CNP CREDIT', 'BNPH RUPY CNP CREDIT')
          )
        )
        AND tran_code_non_liable_identifier IN (6)
        THEN converted_settled_amount_diff
        ELSE 0
      END
    ) AS cnp_keyed_turnover,
    SUM(
      CASE
        WHEN credit IN ('LIABLE CARD TYPES LIABLE LOGIC1 CREDIT', 'LIABLE CARD TYPES LIABLE LOGIC CREDIT')
        AND tran_code_non_liable_identifier = 4
        THEN converted_settled_amount_diff
        ELSE 0
      END
    ) AS refund_amount
  FROM cte_4 AS cte_4
  GROUP BY
    merchant_number_guid,
    corp_guid,
    month,
    settle_alpha_currency_code_diff,
    year
)
SELECT
  *, current_timestamp() as updated_at
FROM cte_5 AS cte_5