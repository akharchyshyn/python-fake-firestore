#!/usr/bin/env bash
set -euo pipefail

if ! command -v gcloud >/dev/null 2>&1; then
  echo "gcloud not found. Install Google Cloud SDK and retry."
  exit 1
fi

if ! command -v java >/dev/null 2>&1; then
  echo "Java not found. Install a JRE/JDK 8+ and ensure java is on PATH."
  exit 1
fi

if ! command -v poetry >/dev/null 2>&1; then
  echo "poetry not found. Install Poetry and retry."
  exit 1
fi

HOSTPORT="${FIRESTORE_EMULATOR_HOST:-localhost:8080}"
PROJECT="${GOOGLE_CLOUD_PROJECT:-demo-test}"
LOG_FILE="${FIRESTORE_EMULATOR_LOG:-/tmp/firestore-emulator.log}"
PORT="${HOSTPORT##*:}"

if lsof -nP -iTCP:"${PORT}" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "Port ${PORT} is already in use. Set FIRESTORE_EMULATOR_HOST to a free port."
  exit 1
fi

echo "Starting Firestore emulator on ${HOSTPORT} for project ${PROJECT}..."
gcloud beta emulators firestore start --host-port="${HOSTPORT}" --project="${PROJECT}" --quiet >"${LOG_FILE}" 2>&1 &
EMULATOR_PID=$!

cleanup() {
  if kill -0 "${EMULATOR_PID}" >/dev/null 2>&1; then
    kill -- -"${EMULATOR_PID}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

for _ in $(seq 1 30); do
  if lsof -nP -iTCP:"${PORT}" -sTCP:LISTEN >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

export FIRESTORE_BACKEND=emulator
export FIRESTORE_EMULATOR_HOST="${HOSTPORT}"
export GOOGLE_CLOUD_PROJECT="${PROJECT}"

echo "Running contract tests against emulator..."
poetry run pytest tests/contract
