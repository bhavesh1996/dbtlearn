{% macro unload_data_to_snowflake() %}

    {{ log("Unloading data", True) }}    

    {% set copy_statement %}
      COPY INTO 'gcs://gcp_dbtpoc/outgoing_file/wwcredit_ap_merchant_data.csv'
      FROM (select * from wwcredit.dev_consumption.wwcredit_ap_merchant_data)
      STORAGE_INTEGRATION = gcs_int, 
      OVERWRITE = TRUE,
      HEADER= TRUE,
      SINGLE = TRUE,
      FILE_FORMAT = ( FORMAT_NAME = 'my_csv_unload_format' );
    {% endset %}
    
    {% do run_query(copy_statement) %}

    {{ log("Unloaded data", True) }}

{% endmacro %}