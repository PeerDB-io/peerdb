#!/bin/sh
# Shared flow-api readiness and HTTP helpers for peer setup.
# Requires FLOW_API_HTTP_PORT to be set (or defaults to 8113).

FLOW_API_HTTP_PORT="${FLOW_API_HTTP_PORT:-8113}"

wait_for_flow_api() {
  echo "Waiting for flow-api and its peer catalog..."
  _flow_api_attempt=0
  # Listing peers checks both the HTTP API and access to the catalog's peers table.
  until curl --silent --show-error --fail --output /dev/null \
    --connect-timeout 2 --max-time 10 \
    "http://localhost:$FLOW_API_HTTP_PORT/v1/peers/list"; do
    _flow_api_attempt=$((_flow_api_attempt + 1))
    if [ "$_flow_api_attempt" -ge 30 ]; then
      echo "Timed out waiting for flow-api and its peer catalog." >&2
      return 1
    fi
    sleep 2
  done
}

call_api() {
  _call_api_method="$1"
  _call_api_endpoint="$2"
  _call_api_payload="$3"

  echo "Calling API: $_call_api_method $_call_api_endpoint"

  if response=$(curl --silent --show-error --fail-with-body \
    --connect-timeout 2 --max-time 30 \
    -X "$_call_api_method" "http://localhost:$FLOW_API_HTTP_PORT$_call_api_endpoint" \
    -H "Content-Type: application/json" \
    -d "$_call_api_payload"); then
    echo "Response: $response"
    # Peer creation can report FAILED in an otherwise successful HTTP response.
    if [ "$_call_api_endpoint" = "/v1/peers/create" ]; then
      printf '%s' "$response" | jq -e '.status == "CREATED" or .status == 1' >/dev/null || return 1
    fi
    return 0
  fi

  echo "API call failed: $_call_api_method $_call_api_endpoint. Response: $response" >&2
  return 1
}
