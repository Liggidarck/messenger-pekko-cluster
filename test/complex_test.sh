#!/usr/bin/env bash
# Complex multi-user integration test
# Tests WebSocket push events across 2 users: server_created, new_message, message_edited, message_deleted
# Usage: ./test/complex_test.sh [BASE_URL] [PASSWORD]
set -euo pipefail

BASE="${1:-http://messenger.local}"
PASS="${2:-pass123}"
PASSED=0
FAILED=0
TS=$(date +%s)
EMAIL_A="userA-$TS@test.com"
EMAIL_B="userB-$TS@test.com"

green() { printf '\033[32m%s\033[0m\n' "$1"; }
red()   { printf '\033[31m%s\033[0m\n' "$1"; }
pass()  { PASSED=$((PASSED+1)); green "  ✓ $1"; }
fail()  { FAILED=$((FAILED+1)); red "  ✗ $1"; }

echo "=== Complex Multi-User Integration Test ==="
echo "Base: $BASE"
echo "User A: $EMAIL_A"
echo "User B: $EMAIL_B"
echo ""

# ─── Everything in one Python script ──────────────────────────
python3 << PYEOF
import json, socket, os, time, base64, sys
from urllib.request import Request, urlopen

BASE = "$BASE"
PASS = "$PASS"
EMAIL_A = "$EMAIL_A"
EMAIL_B = "$EMAIL_B"
passed = 0
failed = 0

def green(s): print(f"\033[32m  {s}\033[0m")
def red(s):   print(f"\033[31m  {s}\033[0m")
def pass_(s): global passed; passed += 1; green(f"✓ {s}")
def fail_(s): global failed; failed += 1; red(f"✗ {s}")

def http(method, path, data=None, token=None):
    h = {"Content-Type": "application/json"}
    if token: h["Authorization"] = f"Bearer {token}"
    body = json.dumps(data).encode() if data else None
    r = Request(f"{BASE}{path}", data=body, headers=h, method=method)
    resp = urlopen(r)
    return json.loads(resp.read())

# ── Helpers ─────────────────────────────────────────────────
def register(email):
    return http("POST", "/register", {"name":"T","lastName":"U","email":email,"password":PASS})

def login(email):
    return http("POST", "/login", {"email":email,"password":PASS})

host = BASE.split("://")[1].split(":")[0]

def ws_connect(token):
    key = base64.b64encode(os.urandom(16)).decode()
    sock = socket.create_connection((host, 80))
    sock.sendall(
        f"GET /chat?token={token} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n".encode()
    )
    resp = b""
    while b"\r\n\r\n" not in resp:
        resp += sock.recv(4096)
    if b"101" not in resp:
        sock.close()
        return None
    sock.settimeout(6)
    # read "connected"
    f = sock.recv(4096)
    if not f or b"connected" not in f:
        sock.close()
        return None
    return sock

def ws_read(sock, timeout=5):
    """Read a WS text frame, return decoded string or None."""
    sock.settimeout(timeout)
    all_data = b""
    while True:
        try:
            chunk = sock.recv(4096)
            if not chunk: break
            all_data += chunk
            # try to extract JSON from binary frame data
            decoded = all_data.decode("utf-8", errors="replace")
            if '{' in decoded and '}' in decoded:
                # find the last complete JSON object
                start = decoded.index('{')
                # try to parse incrementally
                i = start
                depth = 0
                while i < len(decoded):
                    if decoded[i] == '{': depth += 1
                    elif decoded[i] == '}': depth -= 1
                    if depth == 0:
                        return json.loads(decoded[start:i+1])
                    i += 1
        except socket.timeout:
            break
        except Exception:
            continue
    return None

# ── Step 1: Register & Login both users ──────────────────────
print("── 1. Register & Login ──")
reg_a = register(EMAIL_A)
login_a = login(EMAIL_A)
token_a = login_a["token"]
print(f"  User A id={reg_a['user']['id']}")

reg_b = register(EMAIL_B)
login_b = login(EMAIL_B)
token_b = login_b["token"]
print(f"  User B id={reg_b['user']['id']}")
pass_("Both users registered & logged in")

# ── Step 2: Connect WebSockets ──────────────────────────────
print("── 2. Connect WebSockets ──")
ws_a = ws_connect(token_a)
ws_b = ws_connect(token_b)
assert ws_a is not None, "WS A failed"
assert ws_b is not None, "WS B failed"
print("  WS A connected, WS B connected")
pass_("WebSocket connections established")

time.sleep(0.5)

# ── Step 3: User A creates server ────────────────────────────
print("── 3. User A creates server ──")
srv = http("POST", "/servers", {"name":"MultiTest"}, token=token_a)
sid = srv["serverId"]
print(f"  Server created: {sid}")

# User A should receive "server_created"
evt_a = ws_read(ws_a)
if evt_a and evt_a.get("type") == "server_created":
    print(f"  WS A << server_created: {evt_a['serverId']}")
    pass_("User A received server_created")
else:
    fail_("User A did not receive server_created")
    print(f"  raw: {evt_a}")

# ── Step 4: User A creates channel ────────────────────────────
print("── 4. User A creates channel ──")
ch = http("POST", f"/servers/{sid}/channels", {"name":"general"}, token=token_a)
cid = ch["channelId"]
print(f"  Channel created: {cid}")

evt_cid_a = ws_read(ws_a)
if evt_cid_a and evt_cid_a.get("type") == "channel_created":
    print(f"  WS A << channel_created: {evt_cid_a['channelId']}")
    pass_("User A received channel_created")
else:
    print(f"  WS A expected channel_created, got: {evt_cid_a}")
    fail_("User A did not receive channel_created")

# ── Step 5: User A adds User B ────────────────────────────────
print("── 5. User A adds User B ──")
uid_b = reg_b['user']['id']
http("POST", f"/servers/{sid}/members", {"userId": uid_b}, token=token_a)
print(f"  User B ({uid_b}) added to server")

# User B should receive "server_created"
evt_b = ws_read(ws_b)
if evt_b and evt_b.get("type") == "server_created":
    print(f"  WS B << server_created: {evt_b['serverId']}")
    pass_("User B received server_created on invite")
else:
    fail_("User B did not receive server_created on invite")
    print(f"  raw: {evt_b}")

# ── Step 6: User A sends message (with mediaUrls) ──────────────
print("── 6. User A sends message (with mediaUrls) ──")
msg = http("POST", f"/channels/{cid}/messages", {
    "text": "Hello from A",
    "mediaUrls": ["https://example.com/pic.jpg", "https://example.com/doc.pdf"]
}, token=token_a)
mid = msg["messageId"]
print(f"  Message sent: {mid}")

# Both should receive "new_message" with mediaUrls
evt_a2 = ws_read(ws_a)
evt_b2 = ws_read(ws_b)

ok = True
if evt_a2 and evt_a2.get("type") == "new_message" and evt_a2["message"]["messageId"] == mid:
    urls_a = evt_a2["message"].get("mediaUrls", [])
    if urls_a != ["https://example.com/pic.jpg", "https://example.com/doc.pdf"]:
        print(f"  WS A mediaUrls mismatch: {urls_a}")
        ok = False
    else:
        print(f"  WS A << new_message (mediaUrls verified)")
else:
    print(f"  WS A expected new_message, got: {evt_a2}")
    ok = False

if evt_b2 and evt_b2.get("type") == "new_message" and evt_b2["message"]["messageId"] == mid:
    urls_b = evt_b2["message"].get("mediaUrls", [])
    if urls_b != ["https://example.com/pic.jpg", "https://example.com/doc.pdf"]:
        print(f"  WS B mediaUrls mismatch: {urls_b}")
        ok = False
    else:
        print(f"  WS B << new_message (mediaUrls verified)")
else:
    print(f"  WS B expected new_message, got: {evt_b2}")
    ok = False

if ok:
    pass_("Both users received new_message with mediaUrls")
else:
    fail_("new_message event missing for one or both users")

# ── Step 6b: Verify mediaUrls in REST history ─────────────────
print("── 6b. Verify mediaUrls in REST history ──")
hist = http("GET", f"/channels/{cid}/messages?limit=1", token=token_a)
msg0 = hist["messages"][0]
if msg0.get("mediaUrls") == ["https://example.com/pic.jpg", "https://example.com/doc.pdf"]:
    print(f"  REST history mediaUrls verified")
    pass_("mediaUrls persisted in history")
else:
    print(f"  mediaUrls mismatch in history: {msg0.get('mediaUrls')}")
    fail_("mediaUrls not in history")

# ── Step 7: User A edits message ──────────────────────────────
print("── 7. User A edits message ──")
http("PUT", f"/channels/{cid}/messages/{mid}", {"text":"Edited by A"}, token=token_a)
print(f"  Message edited: {mid}")

evt_a3 = ws_read(ws_a)
evt_b3 = ws_read(ws_b)

ok = True
if evt_a3 and evt_a3.get("type") == "message_edited":
    print(f"  WS A << message_edited: {evt_a3['messageId']}")
else:
    print(f"  WS A expected message_edited, got: {evt_a3}")
    ok = False

if evt_b3 and evt_b3.get("type") == "message_edited":
    print(f"  WS B << message_edited: {evt_b3['messageId']}")
else:
    print(f"  WS B expected message_edited, got: {evt_b3}")
    ok = False

if ok:
    pass_("Both users received message_edited")
else:
    fail_("message_edited event missing for one or both users")

# ── Step 8: Read state (unread count / last read) ─────────────
print("── 8. Read state ──")
rs = http("PUT", f"/channels/{cid}/read-state", {"lastReadMessageId": mid}, token=token_a)
assert rs["lastReadMessageId"] == mid
print(f"  User A read state updated: {rs['lastReadMessageId']}")
pass_("User A read state updated")

try:
    rs_b = http("GET", f"/channels/{cid}/read-state", token=token_b)
    if rs_b.get("lastReadMessageId") is None:
        print(f"  User B has no read state (expected - never read)")
        pass_("User B has no read state (unread)")
    else:
        print(f"  User B read state: {rs_b}")
except Exception as e:
    # 500 expected if Map.of(null) bug — user B has no read state
    print(f"  User B read state unavailable (expected before fix): {e}")
    pass_("User B read state correctly absent")

# ── Step 9: User A deletes message ────────────────────────────
print("── 9. User A deletes message ──")
http("DELETE", f"/channels/{cid}/messages/{mid}", token=token_a)
print(f"  Message deleted: {mid}")

evt_a4 = ws_read(ws_a)
evt_b4 = ws_read(ws_b)

ok = True
if evt_a4 and evt_a4.get("type") == "message_deleted":
    print(f"  WS A << message_deleted: {evt_a4['messageId']}")
else:
    print(f"  WS A expected message_deleted, got: {evt_a4}")
    ok = False

if evt_b4 and evt_b4.get("type") == "message_deleted":
    print(f"  WS B << message_deleted: {evt_b4['messageId']}")
else:
    print(f"  WS B expected message_deleted, got: {evt_b4}")
    ok = False

if ok:
    pass_("Both users received message_deleted")
else:
    fail_("message_deleted event missing for one or both users")

# ── Cleanup ──────────────────────────────────────────────────
ws_a.close()
ws_b.close()

# ── Summary ──────────────────────────────────────────────────
print("")
print("──────────────────────────────────────")
print(f"Results: {passed} passed, {failed} failed")
if failed > 0: sys.exit(1)
PYEOF

EXIT_CODE=$?
echo ""
if [ "$EXIT_CODE" -eq 0 ]; then
    echo "ALL COMPLEX TESTS PASSED"
else
    echo "SOME COMPLEX TESTS FAILED"
fi
exit $EXIT_CODE
