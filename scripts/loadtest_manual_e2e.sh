#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
ADMIN_KEY="${ADMIN_KEY:-}"
POLL_SECONDS="${POLL_SECONDS:-3}"
POLL_MAX_ATTEMPTS="${POLL_MAX_ATTEMPTS:-120}"
EXPECT_COOLDOWN="${EXPECT_COOLDOWN:-1}"
START_PAYLOAD='{"scenario":"default","preset":"quick","note":"manual-e2e"}'

LAST_STATUS=""
LAST_BODY_FILE=""

cleanup() {
  if [[ -n "${LAST_BODY_FILE}" && -f "${LAST_BODY_FILE}" ]]; then
    rm -f "${LAST_BODY_FILE}"
  fi
}
trap cleanup EXIT

fail() {
  echo "ERROR: $*" >&2
  if [[ -n "${LAST_BODY_FILE}" && -f "${LAST_BODY_FILE}" ]]; then
    echo "Response body:" >&2
    cat "${LAST_BODY_FILE}" >&2
  fi
  exit 1
}

require_value() {
  local name="$1"
  local value="$2"
  if [[ -z "${value}" ]]; then
    echo "Missing required value: ${name}" >&2
    exit 1
  fi
}

api_call() {
  local method="$1"
  local path="$2"
  local payload="${3:-}"

  if [[ -n "${LAST_BODY_FILE}" && -f "${LAST_BODY_FILE}" ]]; then
    rm -f "${LAST_BODY_FILE}"
  fi
  LAST_BODY_FILE="$(mktemp)"

  if [[ -n "${payload}" ]]; then
    LAST_STATUS="$(
      curl -sS \
        -o "${LAST_BODY_FILE}" \
        -w "%{http_code}" \
        -X "${method}" \
        "${BASE_URL}${path}" \
        -H "Content-Type: application/json" \
        -H "X-Admin-Key: ${ADMIN_KEY}" \
        -d "${payload}"
    )"
  else
    LAST_STATUS="$(
      curl -sS \
        -o "${LAST_BODY_FILE}" \
        -w "%{http_code}" \
        -X "${method}" \
        "${BASE_URL}${path}" \
        -H "X-Admin-Key: ${ADMIN_KEY}"
    )"
  fi
}

extract_json_string() {
  local key="$1"
  sed -n "s/.*\"${key}\":[[:space:]]*\"\\([^\"]*\\)\".*/\\1/p" "${LAST_BODY_FILE}" | head -n 1
}

extract_json_number() {
  local key="$1"
  sed -n "s/.*\"${key}\":[[:space:]]*\\([0-9][0-9]*\\).*/\\1/p" "${LAST_BODY_FILE}" | head -n 1
}

require_value "ADMIN_KEY" "${ADMIN_KEY}"

echo "== Nexus loadtest manual E2E API checks =="
echo "BASE_URL=${BASE_URL}"

echo
echo "1) Start run with valid key (expect 202)"
api_call "POST" "/ops/loadtest/start" "${START_PAYLOAD}"
if [[ "${LAST_STATUS}" != "202" ]]; then
  fail "Expected status 202 for first start, got ${LAST_STATUS}"
fi
RUN_ID="$(extract_json_number "run_id")"
require_value "run_id" "${RUN_ID}"
echo "OK: first start accepted, run_id=${RUN_ID}"

echo
echo "2) Start again while active (expect 409)"
api_call "POST" "/ops/loadtest/start" "${START_PAYLOAD}"
if [[ "${LAST_STATUS}" != "409" ]]; then
  fail "Expected status 409 for second start, got ${LAST_STATUS}"
fi
echo "OK: second start blocked with 409"

echo
echo "3) Poll run status until terminal state (completed/aborted)"
RUN_STATUS=""
for ((i = 1; i <= POLL_MAX_ATTEMPTS; i++)); do
  api_call "GET" "/ops/loadtest/${RUN_ID}"
  if [[ "${LAST_STATUS}" != "200" ]]; then
    fail "Expected status 200 while polling run status, got ${LAST_STATUS}"
  fi
  RUN_STATUS="$(extract_json_string "status")"
  if [[ "${RUN_STATUS}" == "completed" || "${RUN_STATUS}" == "aborted" ]]; then
    echo "OK: run reached terminal state: ${RUN_STATUS}"
    break
  fi
  echo "   attempt ${i}/${POLL_MAX_ATTEMPTS}: status=${RUN_STATUS:-unknown}"
  sleep "${POLL_SECONDS}"
done

if [[ "${RUN_STATUS}" != "completed" && "${RUN_STATUS}" != "aborted" ]]; then
  fail "Run did not reach terminal state within timeout"
fi

echo
echo "4) Verify cooldown behavior (expect 429 with retry hint)"
api_call "POST" "/ops/loadtest/start" "${START_PAYLOAD}"
if [[ "${LAST_STATUS}" == "429" ]]; then
  if grep -qi "retry in" "${LAST_BODY_FILE}"; then
    echo "OK: cooldown enforced with retry countdown hint"
  else
    fail "Cooldown response missing 'retry in' hint"
  fi
else
  if [[ "${EXPECT_COOLDOWN}" == "1" ]]; then
    fail "Expected 429 cooldown response, got ${LAST_STATUS}"
  fi
  echo "WARN: cooldown not enforced (status=${LAST_STATUS}); EXPECT_COOLDOWN=0 so continuing"
fi

echo
echo "API checklist passed."
echo "Next manual UI checks:"
echo "- Confirm completed run shows FINAL SCORE and three insight bullets."
echo "- Confirm upstream error state shows actionable hint by testing with an invalid K6 upstream token."
