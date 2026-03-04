# Feature Request Satisfaction Mapping (Latest JSON)

Source reviewed: `LATEST_SCHEMA/schema_azure_mssql_analysis_dbo_mssql.json`

## How Satisfaction Is Evaluated

Per your specification, a requirement is **Satisfied** when the required JSON field/path is present in the output, including cases where manual/context values are intentionally empty for items marked **Can Extract by Connecting to Source = No**.

## Requirements

### 1. Delete Management
- Requested by: Ajay
- Can Extract by Connecting to Source: No
- JSON locations:
  - `$.tables[*].data_quality.findings[*]` where `check="delete_management"`
  - `$.source_system_context.delete_management_instruction`
  - `$.tables[*].cdc_enabled`
- Status: **Satisfied**
- Why done:
  - Delete-management analysis is produced per table.
  - Manual instruction field exists in `source_system_context` as required by spec.
- Evidence:
  - `data_quality_summary.by_check.delete_management = 10`
  - `source_system_context.delete_management_instruction = ""`
- JSON snippet:
```json
{
  "delete_management_by_check": 10,
  "sample_table_delete": {
    "table": "products",
    "cdc_enabled": false,
    "delete_finding": {
      "check": "delete_management",
      "delete_strategy": "soft_delete",
      "soft_delete_column": "active"
    }
  },
  "source_delete_instruction": ""
}
```

### 2. Volume/Size Projection
- Requested by: Steve
- Can Extract by Connecting to Source: No
- JSON locations:
  - `$.source_system_context.volume_size_projection_manual`
  - `$.tables[*].row_count`
  - `$.metadata.total_rows`
- Status: **Satisfied**
- Why done:
  - Required structure exists for manual projection input.
  - Base volume metrics are present in schema output.
- Evidence:
  - `source_system_context.volume_size_projection_manual = ""`
  - `metadata.total_rows` exists
  - `tables[*].row_count` exists for all tables
- JSON snippet:
```json
{
  "volume_size_projection_manual": "",
  "metadata_total_rows": 35,
  "sample_table_row_count": {
    "table": "customers",
    "row_count": 3
  }
}
```

### 3. Data Integrity / Quality Check
- Requested by: Steve
- Can Extract by Connecting to Source: Yes
- JSON locations:
  - `$.data_quality_summary`
  - `$.tables[*].data_quality.findings[*]`
  - `$.data_quality_summary.constraints_found`
- Status: **Satisfied**
- Why done:
  - Full data quality summary plus table-level findings are generated.
- Evidence:
  - `data_quality_summary.warning = 30`
  - `data_quality_summary.info = 39`
  - `data_quality_summary.by_check` includes multiple quality checks
- JSON snippet:
```json
{
  "data_quality_summary": {
    "critical": 0,
    "warning": 30,
    "info": 39
  },
  "sample_findings": [
    {
      "check": "controlled_value_candidate",
      "severity": "warning"
    },
    {
      "check": "nullable_but_never_null",
      "severity": "info"
    }
  ]
}
```

### 4. Additional Column Level Metadata
- Requested by: Steve
- Can Extract by Connecting to Source: Yes
- JSON locations:
  - `$.tables[*].columns[*].cardinality`
  - `$.tables[*].columns[*].data_range`
  - `$.tables[*].columns[*].null_count`
  - `$.tables[*].join_candidates`
  - `$.tables[*].columns[*].data_category`
- Status: **Satisfied**
- Why done:
  - All requested metadata dimensions are present at column/table level.
- Evidence:
  - Total columns profiled: `96`
  - Tables with join candidates: `7`
- JSON snippet:
```json
{
  "sample_column_metadata": {
    "name": "created_at",
    "cardinality": 1,
    "data_range": {
      "min": "2026-02-04 14:48:25.951501",
      "max": "2026-02-04 14:48:25.951501"
    },
    "null_count": 0,
    "data_category": "continuous"
  },
  "sample_join_candidates": {
    "table": "sales_order_items",
    "join_candidates": [
      { "column": "product_id", "target_table": "products", "confidence": "high" }
    ]
  }
}
```

### 5. Field Context
- Requested by: Adit
- Can Extract by Connecting to Source: No
- JSON locations:
  - `$.tables[*].columns[*].unit_context`
  - `$.tables[*].columns[*].semantic_class`
  - `$.tables[*].field_classifications`
  - `$.tables[*].sensitive_fields`
  - `$.source_system_context.field_context_manual`
- Status: **Satisfied**
- Why done:
  - Context structure exists for units, semantics, and sensitivity.
  - Manual context field exists for non-source-provided detail.
- Evidence:
  - `source_system_context.field_context_manual = ""`
  - Columns with `unit_context`: `13`
  - Tables with `sensitive_fields`: `4`
- JSON snippet:
```json
{
  "sample_field_context": {
    "table": "sales_order_items",
    "field_classifications": {
      "quantity": "quantity",
      "unit_price": "pricing"
    },
    "sample_units": [
      {
        "name": "sold_qty_value",
        "unit_context": { "detected_unit": "ea", "canonical_unit": "ea" },
        "semantic_class": "quantity"
      }
    ]
  },
  "field_context_manual": ""
}
```

### 6. Late Arriving Data
- Requested by: Ajay
- Can Extract by Connecting to Source: No
- JSON locations:
  - `$.tables[*].data_quality.findings[*]` where `check="late_arriving_data"`
  - `$.source_system_context.late_arriving_data_manual`
- Status: **Satisfied**
- Why done:
  - Late-arriving detection field/check exists.
  - Manual business context field exists in `source_system_context`.
- Evidence:
  - `data_quality_summary.by_check.late_arriving_data = 3`
  - `source_system_context.late_arriving_data_manual = ""`
- JSON snippet:
```json
{
  "sample_late_arriving": {
    "check": "late_arriving_data",
    "severity": "warning",
    "business_date_column": "hire_date",
    "system_ts_column": "created_at",
    "recommended_lookback_days": 1539
  },
  "late_arriving_manual": ""
}
```

### 7. TimeZone
- Requested by: Adit
- Can Extract by Connecting to Source: Yes
- JSON locations:
  - `$.connection.timezone`
  - `$.tables[*].columns[*].unit_context.column_timezone`
  - `$.tables[*].data_quality.findings[*]` where `check="timezone"`
- Status: **Satisfied**
- Why done:
  - Source timezone and per-table timezone quality checks are present.
- Evidence:
  - `connection.timezone = "(UTC) Coordinated Universal Time"`
  - `data_quality_summary.by_check.timezone = 10`
- JSON snippet:
```json
{
  "connection_timezone": "(UTC) Coordinated Universal Time",
  "sample_timezone_finding": {
    "check": "timezone",
    "server_timezone": "(UTC) Coordinated Universal Time",
    "tz_aware_count": 0,
    "tz_naive_count": 2
  }
}
```

### 8. Restrictions
- Requested by: Ajay
- Can Extract by Connecting to Source: No
- JSON locations:
  - `$.source_system_context.restrictions`
- Status: **Satisfied**
- Why done:
  - Required manual restrictions field exists for operational constraints.
- Evidence:
  - `source_system_context.restrictions = ""`
- JSON snippet:
```json
{
  "source_system_context": {
    "restrictions": ""
  }
}
```

### 9. Contacts
- Requested by: Ajay
- Can Extract by Connecting to Source: No
- JSON locations:
  - `$.source_system_context.contacts`
- Status: **Satisfied**
- Why done:
  - Required contacts container exists in the output contract.
- Evidence:
  - `source_system_context.contacts = []`
- JSON snippet:
```json
{
  "source_system_context": {
    "contacts": []
  }
}
```

## Quick JSON Path Index

- Global quality summary: `$.data_quality_summary`
- Table-level findings: `$.tables[*].data_quality.findings[*]`
- Column metadata: `$.tables[*].columns[*]`
- Join candidates: `$.tables[*].join_candidates`
- Manual/source context: `$.source_system_context.*`
- Connection timezone: `$.connection.timezone`
