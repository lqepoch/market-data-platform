#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SHARED_TARGET="${MDP_SHARED_TARGET:-$PROJECT_ROOT/target}"
MDP_BIN="$SHARED_TARGET/debug/market-data-platform"
RUNTIME_IMAGE_ID="${MDP_RUNTIME_IMAGE_ID:-}"

if [[ ! "$RUNTIME_IMAGE_ID" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "set MDP_RUNTIME_IMAGE_ID to an already-cached immutable Linux image ID" >&2
  exit 2
fi
if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required for the offline container smoke" >&2
  exit 2
fi
if ! docker image inspect "$RUNTIME_IMAGE_ID" >/dev/null 2>&1; then
  echo "the requested runtime image ID is not present locally; this script never pulls images" >&2
  exit 2
fi

CARGO_TARGET_DIR="$SHARED_TARGET" CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-2}" \
  cargo +1.98.1 build --offline --locked --bin market-data-platform
if [[ ! -x "$MDP_BIN" ]]; then
  echo "MDP host-built executable was not produced at the requested path" >&2
  exit 1
fi
if ! "$MDP_BIN" serve --help >/dev/null; then
  echo "the shared target executable does not contain the current HTTP service command" >&2
  exit 1
fi

RUN_ID="$$"
BASE_TAG="mdp-local-runtime-$RUN_ID:cached"
SMOKE_TAG="mdp-http-local-smoke-$RUN_ID:local"
CONTAINER="mdp-http-smoke-$RUN_ID"
TERM_CONTAINER="mdp-http-term-smoke-$RUN_ID"
TEMP_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/mdp-http-smoke.XXXXXX")"

cleanup() {
  docker rm --force "$TERM_CONTAINER" >/dev/null 2>&1 || true
  if [[ -n "${LOAD_PID:-}" ]]; then
    wait "$LOAD_PID" 2>/dev/null || true
  fi
  docker rm --force "$CONTAINER" >/dev/null 2>&1 || true
  docker image rm "$SMOKE_TAG" >/dev/null 2>&1 || true
  docker image rm "$BASE_TAG" >/dev/null 2>&1 || true
  rm -rf -- "$TEMP_ROOT"
}
trap cleanup EXIT

CONTEXT="$TEMP_ROOT/context"
mkdir -m 0755 "$CONTEXT"
cp -- "$MDP_BIN" "$CONTEXT/mdp"
chmod 0755 "$CONTEXT/mdp"
SOURCE_SHA256="$(sha256sum "$MDP_BIN" | cut -d ' ' -f 1)"
COPIED_SHA256="$(sha256sum "$CONTEXT/mdp" | cut -d ' ' -f 1)"
if [[ "$SOURCE_SHA256" != "$COPIED_SHA256" ]]; then
  echo "host executable and container build context SHA-256 differ" >&2
  exit 1
fi

docker tag "$RUNTIME_IMAGE_ID" "$BASE_TAG"
docker buildx build \
  --pull=false \
  --network=none \
  --load \
  --tag "$SMOKE_TAG" \
  --build-arg "MDP_RUNTIME_BASE=$BASE_TAG" \
  --file "$PROJECT_ROOT/Dockerfile" \
  "$CONTEXT"

REPLAY_ROOT="$TEMP_ROOT/replay"
"$MDP_BIN" synthetic --output "$REPLAY_ROOT" >/dev/null
chmod 0755 "$TEMP_ROOT" "$REPLAY_ROOT"
chmod -R a+rX "$REPLAY_ROOT/local-test-store"

TERMINAL_TEST_KEY="mdp-container-smoke-terminal-key-not-a-credential-0001"
RESEARCH_TEST_KEY="mdp-container-smoke-research-key-not-a-credential-0002"
docker run --detach \
  --name "$CONTAINER" \
  --network none \
  --read-only \
  --memory 2g \
  --cpus 2 \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,size=16m,uid=10001,gid=10001,mode=0700 \
  --tmpfs /var/cache/mdp:rw,noexec,nosuid,nodev,size=64m,uid=10001,gid=10001,mode=0700 \
  --mount "type=bind,source=$REPLAY_ROOT/local-test-store,target=/data/local-test-store,readonly" \
  --env "MDP_TERMINAL_JWT_SECRET=$TERMINAL_TEST_KEY" \
  --env "MDP_RESEARCH_JWT_SECRET=$RESEARCH_TEST_KEY" \
  "$SMOKE_TAG" \
  serve --bind 0.0.0.0:8088 \
  --local-test-root /data/local-test-store \
  --cache-dir /var/cache/mdp >/dev/null

sleep 0.5
if [[ "$(docker inspect --format '{{.State.Status}}' "$CONTAINER")" != "running" ]]; then
  echo "MDP container exited during startup:" >&2
  docker logs "$CONTAINER" >&2 || true
  exit 1
fi

HOST_ARCH="$(uname -m)"
HOST_GLIBC="$(getconf GNU_LIBC_VERSION | awk '{print $2}')"
RUNTIME_FACTS="$(docker exec "$CONTAINER" python3 -c 'import platform; print(platform.machine(), *platform.libc_ver())')"
read -r RUNTIME_ARCH RUNTIME_LIBC RUNTIME_GLIBC <<< "$RUNTIME_FACTS"
if [[ "$HOST_ARCH" != "$RUNTIME_ARCH" || "$RUNTIME_LIBC" != "glibc" ]]; then
  echo "host/runtime architecture or runtime libc is unsupported: host=$HOST_ARCH runtime=$RUNTIME_FACTS" >&2
  exit 1
fi
echo "container runtime ABI: host arch=$HOST_ARCH glibc=$HOST_GLIBC; runtime arch=$RUNTIME_ARCH glibc=$RUNTIME_GLIBC"

if ! docker exec -i \
  --env "MDP_EXPECTED_BINARY_SHA256=$SOURCE_SHA256" \
  --env "MDP_TERMINAL_JWT_SECRET=$TERMINAL_TEST_KEY" \
  "$CONTAINER" python3 - <<'PY'
import base64
import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.request

base = "http://127.0.0.1:8088"
expected_hash = os.environ["MDP_EXPECTED_BINARY_SHA256"]
with open("/usr/local/bin/mdp", "rb") as executable:
    assert hashlib.sha256(executable.read()).hexdigest() == expected_hash

for _ in range(50):
    try:
        with urllib.request.urlopen(base + "/healthz", timeout=1) as response:
            assert response.status == 200
            assert json.load(response)["status"] == "alive"
        break
    except (OSError, urllib.error.URLError):
        time.sleep(0.2)
else:
    raise AssertionError("HTTP liveness did not start in time")

with urllib.request.urlopen(base + "/readyz", timeout=2) as response:
    ready = json.load(response)
    assert response.status == 200
    assert ready["market_ready"] is False
    assert ready["source_entitlement"] == "unverified"

dataset_id = "synthetic-2026-10-08-four-bars-parquet-v3-bars-1m-v1"
path = f"/v1/datasets/{dataset_id}/bars?namespace=diagnostic&symbol=QQQ"
try:
    urllib.request.urlopen(base + path, timeout=2)
except urllib.error.HTTPError as error:
    assert error.code == 401
else:
    raise AssertionError("unauthenticated data request was accepted")
assert not os.path.exists(f"/var/cache/mdp/diagnostic/{dataset_id}")

def b64url(value):
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")

now = int(time.time())
header = {"alg": "HS256", "kid": "mdp-terminal", "typ": "JWT"}
claims = {
    "iss": "eqoboard-openterminal",
    "aud": "lqepoch-market-data",
    "sub": "container-smoke",
    "idp_iss": "https://identity.example.test",
    "jti": "container-smoke-1",
    "iat": now,
    "exp": now + 30,
    "scope": ["market:read"],
}
unsigned = b".".join((
    b64url(json.dumps(header, separators=(",", ":")).encode()).encode(),
    b64url(json.dumps(claims, separators=(",", ":")).encode()).encode(),
))
signature = hmac.new(
    os.environ["MDP_TERMINAL_JWT_SECRET"].encode(), unsigned, hashlib.sha256
).digest()
token = unsigned.decode() + "." + b64url(signature)
request = urllib.request.Request(base + path, headers={"Authorization": "Bearer " + token})
with urllib.request.urlopen(request, timeout=30) as response:
    body = response.read()
    assert response.status == 200
    assert response.headers["Cache-Control"] == "no-store"
    assert response.headers["Content-Type"] == "application/json"
payload = json.loads(body)
assert payload["summary"]["schema_id"] == "lqepoch.us_equity_trade_bar_1m.v1"
assert payload["summary"]["namespace"] == "diagnostic"
assert payload["summary"]["source"]["provider"] == "synthetic"
assert payload["summary"]["source"]["feed"] == "synthetic"
assert payload["summary"]["source"]["entitlement"] == "unknown"
assert payload["summary"]["row_count"] == "4"
assert payload["summary"]["returned_rows"] == "4"
assert len(payload["rows"]) == 4
assert os.path.isfile(f"/var/cache/mdp/diagnostic/{dataset_id}/.cache-receipt.json")
print("container HTTP smoke passed: hash, startup, readiness, unauthenticated rejection, authenticated synthetic V1 query")
PY
then
  echo "MDP container probe failed; service logs follow:" >&2
  docker logs "$CONTAINER" >&2 || true
  exit 1
fi

docker kill --signal=INT "$CONTAINER" >/dev/null
EXIT_STATUS="$(docker wait "$CONTAINER" || true)"
if [[ "$EXIT_STATUS" != "0" ]]; then
  echo "service did not shut down cleanly after the container SIGINT: $EXIT_STATUS" >&2
  docker logs "$CONTAINER" >&2 || true
  exit 1
fi

TERM_REPLAY_ROOT="$TEMP_ROOT/regular-session-replay"
"$MDP_BIN" synthetic --regular-session --output "$TERM_REPLAY_ROOT" >/dev/null
chmod 0755 "$TERM_REPLAY_ROOT"
chmod -R a+rX "$TERM_REPLAY_ROOT/local-test-store"
docker run --detach \
  --name "$TERM_CONTAINER" \
  --network none \
  --read-only \
  --memory 2g \
  --cpus 2 \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,size=16m,uid=10001,gid=10001,mode=0700 \
  --tmpfs /var/cache/mdp:rw,noexec,nosuid,nodev,size=64m,uid=10001,gid=10001,mode=0700 \
  --mount "type=bind,source=$TERM_REPLAY_ROOT/local-test-store,target=/data/local-test-store,readonly" \
  --env "MDP_TERMINAL_JWT_SECRET=$TERMINAL_TEST_KEY" \
  --env "MDP_RESEARCH_JWT_SECRET=$RESEARCH_TEST_KEY" \
  --env "RUST_LOG=info" \
  "$SMOKE_TAG" \
  serve --bind 0.0.0.0:8088 \
  --local-test-root /data/local-test-store \
  --cache-dir /var/cache/mdp >/dev/null

sleep 0.5
if [[ "$(docker inspect --format '{{.State.Status}}' "$TERM_CONTAINER")" != "running" ]]; then
  echo "MDP SIGTERM container exited during startup:" >&2
  docker logs "$TERM_CONTAINER" >&2 || true
  exit 1
fi

cat > "$TEMP_ROOT/term_load.py" <<'PY'
import base64
import hashlib
import hmac
import json
import os
import threading
import time
import urllib.request

base = "http://127.0.0.1:8088"
dataset_id = "synthetic-2026-10-08-full-390-minute-session-parquet-v2-bars-1m-v1"
path = f"/v1/datasets/{dataset_id}/bars?namespace=diagnostic&symbol=QQQ"

def b64url(value):
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")

now = int(time.time())
header = {"alg": "HS256", "kid": "mdp-terminal", "typ": "JWT"}
claims = {
    "iss": "eqoboard-openterminal",
    "aud": "lqepoch-market-data",
    "sub": "container-sigterm-smoke",
    "idp_iss": "https://identity.example.test",
    "jti": "container-sigterm-smoke-1",
    "iat": now,
    "exp": now + 30,
    "scope": ["market:read"],
}
unsigned = b".".join((
    b64url(json.dumps(header, separators=(",", ":")).encode()).encode(),
    b64url(json.dumps(claims, separators=(",", ":")).encode()).encode(),
))
signature = hmac.new(
    os.environ["MDP_TERMINAL_JWT_SECRET"].encode(), unsigned, hashlib.sha256
).digest()
token = unsigned.decode() + "." + b64url(signature)
request = urllib.request.Request(base + path, headers={"Authorization": "Bearer " + token})
stop = threading.Event()

def query_loop():
    while not stop.is_set():
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                if response.status == 200:
                    response.read()
        except OSError:
            if not stop.is_set():
                time.sleep(0.002)

def has_worker():
    try:
        with os.scandir("/proc") as entries:
            for entry in entries:
                if not entry.name.isdigit():
                    continue
                try:
                    command = open(f"/proc/{entry.name}/cmdline", "rb").read()
                except OSError:
                    continue
                if b"parquet-worker" in command:
                    return True
    except OSError:
        return False
    return False

threads = [threading.Thread(target=query_loop, daemon=True) for _ in range(12)]
for thread in threads:
    thread.start()
while not has_worker():
    time.sleep(0.002)
print("ACTIVE_PARQUET_WORKER", flush=True)
while True:
    time.sleep(1)
PY
docker exec --interactive \
  --env "MDP_TERMINAL_JWT_SECRET=$TERMINAL_TEST_KEY" \
  "$TERM_CONTAINER" python3 - \
  < "$TEMP_ROOT/term_load.py" \
  > "$TEMP_ROOT/term_load.log" 2>&1 &
LOAD_PID=$!

WORKER_OBSERVED=false
for _ in $(seq 1 80); do
  if grep -Fq "ACTIVE_PARQUET_WORKER" "$TEMP_ROOT/term_load.log"; then
    WORKER_OBSERVED=true
    break
  fi
  sleep 0.05
done
if [[ "$WORKER_OBSERVED" != true ]]; then
  echo "the authenticated load did not expose an active Parquet worker before SIGTERM" >&2
  docker logs "$TERM_CONTAINER" >&2 || true
  exit 1
fi

docker kill --signal=TERM "$TERM_CONTAINER" >/dev/null
TERM_EXIT_STATUS="$(docker wait "$TERM_CONTAINER" || true)"
wait "$LOAD_PID" || true
if [[ "$TERM_EXIT_STATUS" != "0" ]]; then
  echo "service did not shut down cleanly after SIGTERM during an observed Parquet query: $TERM_EXIT_STATUS" >&2
  docker logs "$TERM_CONTAINER" >&2 || true
  exit 1
fi
TERM_LOGS="$(docker logs "$TERM_CONTAINER" 2>&1)"
if ! grep -Fq "MDP HTTP query supervisor joined all active workers" <<< "$TERM_LOGS"; then
  echo "SIGTERM exit did not confirm the query supervisor joined its workers" >&2
  printf '%s\n' "$TERM_LOGS" >&2
  exit 1
fi

echo "offline container smoke passed: SIGINT and SIGTERM exits were clean; SIGTERM arrived while an authenticated query had an observed Parquet worker; host binary sha256=$SOURCE_SHA256; cached runtime image id=$RUNTIME_IMAGE_ID (host-built runtime smoke only)"
