{{ config(
    materialized='scd2_incremental',
    meta={
        'source_ns':     'bronze_erp__dbo',
        'source_tbl':    'CUSTOMERS',
        'source_bk_col': 'CUSTOMER_ID',
        'target_bk_col': '"SourceCustomerPK"',
    }
) }}

SELECT * FROM {{ ref('vw_DimCustomer') }}
