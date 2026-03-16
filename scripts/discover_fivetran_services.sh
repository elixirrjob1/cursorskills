#!/usr/bin/env bash
# Discover Fivetran destination service names by testing common variations
# Usage: ./scripts/discover_fivetran_services.sh [service_name_to_test]

set -euo pipefail

API_KEY="${FIVETRAN_API_KEY:-}"
API_SECRET="${FIVETRAN_API_SECRET:-}"
GROUP_ID="${FIVETRAN_GROUP_ID:-horns_nozzle}"

if [[ -z "$API_KEY" || -z "$API_SECRET" ]]; then
    echo "Error: FIVETRAN_API_KEY and FIVETRAN_API_SECRET must be set"
    exit 1
fi

SERVICE_TO_TEST="${1:-}"

if [[ -n "$SERVICE_TO_TEST" ]]; then
    echo "Testing service name: $SERVICE_TO_TEST"
    response=$(curl -s -X POST \
        -u "$API_KEY:$API_SECRET" \
        -H "Accept: application/json;version=2" \
        -H "Content-Type: application/json" \
        -d "{
            \"group_id\": \"$GROUP_ID\",
            \"service\": \"$SERVICE_TO_TEST\",
            \"region\": \"AWS_US_EAST_1\",
            \"time_zone_offset\": \"-8\",
            \"config\": {}
        }" \
        "https://api.fivetran.com/v1/destinations")
    
    echo "$response" | python3 -m json.tool 2>/dev/null || echo "$response"
    exit 0
fi

echo "Common Fivetran destination service names:"
echo ""
echo "Testing common PostgreSQL variations..."
echo ""

# Test common PostgreSQL service name variations
for service in postgres postgresql postgres_warehouse postgres_destination pg postgresql_warehouse; do
    echo -n "Testing '$service'... "
    response=$(curl -s -X POST \
        -u "$API_KEY:$API_SECRET" \
        -H "Accept: application/json;version=2" \
        -H "Content-Type: application/json" \
        -d "{
            \"group_id\": \"$GROUP_ID\",
            \"service\": \"$service\",
            \"region\": \"AWS_US_EAST_1\",
            \"time_zone_offset\": \"-8\",
            \"config\": {}
        }" \
        "https://api.fivetran.com/v1/destinations" 2>/dev/null)
    
    if echo "$response" | grep -q "Unsupported service"; then
        echo "❌ Unsupported"
    elif echo "$response" | grep -q "Success\|created"; then
        echo "✅ VALID SERVICE NAME"
        # Clean up test destination if created
        dest_id=$(echo "$response" | python3 -c "import sys, json; d=json.load(sys.stdin); print(d.get('data', {}).get('id', ''))" 2>/dev/null || echo "")
        if [[ -n "$dest_id" && "$dest_id" != "$GROUP_ID" ]]; then
            echo "   (Test destination created, you may want to delete it)"
        fi
    else
        echo "⚠️  Unknown response"
    fi
done

echo ""
echo "Note: This script tests service names. Some may create test destinations."
echo "Check Fivetran UI or API docs for complete list of supported services."
