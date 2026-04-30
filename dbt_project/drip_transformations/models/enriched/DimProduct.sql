{{ config(
    materialized='scd2_incremental',
    meta={
        'source_ns':     'bronze_erp__dbo',
        'source_tbl':    'PRODUCTS',
        'source_bk_col': 'PRODUCT_ID',
        'target_bk_col': '"SourceProductPK"',
    }
) }}

SELECT * FROM {{ ref('vw_DimProduct') }}
