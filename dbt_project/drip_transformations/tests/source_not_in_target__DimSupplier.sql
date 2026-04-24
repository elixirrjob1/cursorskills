-- Singular dbt test: SourceNotInTarget for DimSupplier
--
-- Fails if any primary-key hashes present in the source view (vw_DimSupplier)
-- are missing from the target table (DimSupplier).
--
-- Run locally in dev with:
--   dbt test --select source_not_in_target__DimSupplier --target dev

SELECT
    src.SupplierHashPK
FROM {{ ref('vw_DimSupplier') }} AS src
LEFT JOIN {{ ref('DimSupplier') }} AS tgt
    ON src.SupplierHashPK = tgt.SupplierHashPK
WHERE tgt.SupplierHashPK IS NULL
