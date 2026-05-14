# Test API Endpoints

Current tested endpoints:

- `GET /api/tables`
- `GET /api/{table}?limit=100&offset=0`

Recommended first calls:

```bash
.venv/bin/python scripts/apis/test_api/test_api_reader.py --discover --output api_discovery.json
.venv/bin/python scripts/apis/test_api/test_api_reader.py --path /api/tables --path /api/customers --output api_payloads.json
```

Then normalize to schema contract:

```bash
.venv/bin/python scripts/apis/api_analyzer.py --discovery api_discovery.json --data-dir ./api_data --base-url "$API_BASE_URL" --output schema_api.json
```
