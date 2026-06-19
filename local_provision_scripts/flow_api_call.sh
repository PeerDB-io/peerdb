#!/bin/sh
# Generic flow-api HTTP utility. Source this file to get call_api().
# Requires FLOW_API_HTTP_PORT to be set (or defaults to 8113).

FLOW_API_HTTP_PORT="${FLOW_API_HTTP_PORT:-8113}"

call_api() {
  _call_api_method="$1"
  _call_api_endpoint="$2"
  _call_api_payload="$3"

  echo "Calling API: $_call_api_method $_call_api_endpoint"
  echo "Payload: $_call_api_payload"

  response=$(curl -s -X "$_call_api_method" "http://localhost:$FLOW_API_HTTP_PORT$_call_api_endpoint" \
    -H "Content-Type: application/json" \
    -d "$_call_api_payload")

  echo "Response: $response"
}
