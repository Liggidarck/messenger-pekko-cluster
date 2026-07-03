#!/usr/bin/env bash
# Integration test for messenger-cluster REST API + WebSocket
# Usage: ./test/integration_test.sh [BASE_URL]
#   BASE_URL defaults to http://messenger.local
set -euo pipefail

BASE="${1:-http://messenger.local}"
PASS="${2:-pass123}"
EMAIL="test-$(date +%s)@test.com"
PASSED=0
FAILED=0

green() { printf '\033[32m%s\033[0m\n' "$1"; }
red()   { printf '\033[31m%s\033[0m\n' "$1"; }
pass()  { PASSED=$((PASSED+1)); green "  ✓ $1"; }
fail()  { FAILED=$((FAILED+1)); red "  ✗ $1: $2"; }

request() {
    local method=$1 path=$2; shift 2
    local hdrs=()
    if [ -n "${TOKEN:-}" ]; then
        hdrs+=(-H "Authorization: Bearer $TOKEN")
    fi
    curl -sS -X "$method" "$BASE$path" "${hdrs[@]}" -H 'Content-Type: application/json' "$@"
}

print_resp() {
    local label=$1 resp=$2
    echo "$resp" | python3 -m json.tool 2>/dev/null || echo "$resp"
    echo ""
}

echo "=== Messenger Cluster Integration Test ==="
echo "Base: $BASE  Email: $EMAIL"
echo ""

# ─── 1. Register ──────────────────────────────────────────────
echo "── 1. Register ──"
REG=$(request POST /register -d "{\"name\":\"Test\",\"lastName\":\"User\",\"email\":\"$EMAIL\",\"password\":\"$PASS\"}")
print_resp "POST /register" "$REG"
if echo "$REG" | python3 -c 'import sys,json; d=json.load(sys.stdin); assert "id" in d["user"] and d["user"]["email"] == "'"$EMAIL"'"' 2>/dev/null; then
    pass "Register"
else
    fail "Register" "$REG"
fi

# ─── 2. Login ─────────────────────────────────────────────────
echo "── 2. Login ──"
LOGIN=$(request POST /login -d "{\"email\":\"$EMAIL\",\"password\":\"$PASS\"}")
print_resp "POST /login" "$LOGIN"
TOKEN=$(echo "$LOGIN" | python3 -c 'import sys,json; print(json.load(sys.stdin)["token"])' 2>/dev/null || true)
if [ -n "$TOKEN" ]; then
    pass "Login (token obtained)"
else
    fail "Login" "$LOGIN"
fi

# ─── 3. Create Server ─────────────────────────────────────────
echo "── 3. Create Server ──"
SERVER=$(request POST /servers -d '{"name":"TestServer"}')
print_resp "POST /servers" "$SERVER"
SERVER_ID=$(echo "$SERVER" | python3 -c 'import sys,json; print(json.load(sys.stdin)["serverId"])' 2>/dev/null || true)
if [ -n "$SERVER_ID" ]; then
    pass "Create Server ($SERVER_ID)"
else
    fail "Create Server" "$SERVER"
fi

# ─── 4. List Servers ──────────────────────────────────────────
echo "── 4. List Servers ──"
SERVERS=$(request GET /servers)
print_resp "GET /servers" "$SERVERS"
FOUND=$(echo "$SERVERS" | python3 -c "
import sys,json; d=json.load(sys.stdin)
ids = [s['serverId'] for s in d['servers']]
assert '$SERVER_ID' in ids, f'missing {chr(39)}$SERVER_ID{chr(39)}'
print(len(d['servers']))
" 2>/dev/null || true)
if [ -n "$FOUND" ]; then
    pass "List Servers ($FOUND servers)"
else
    fail "List Servers" "$SERVERS"
fi

# ─── 5. Get Server Profile ────────────────────────────────────
echo "── 5. Get Server Profile ──"
PROFILE=$(request GET "/servers/$SERVER_ID")
print_resp "GET /servers/$SERVER_ID" "$PROFILE"
if echo "$PROFILE" | python3 -c "
import sys,json; d=json.load(sys.stdin)
assert d['serverId'] == '$SERVER_ID'
assert d['name'] == 'TestServer'
" 2>/dev/null; then
    pass "Get Server Profile"
else
    fail "Get Server Profile" "$PROFILE"
fi

# ─── 6. Create Channel ────────────────────────────────────────
echo "── 6. Create Channel ──"
CHANNEL=$(request POST "/servers/$SERVER_ID/channels" -d '{"name":"general"}')
print_resp "POST /servers/$SERVER_ID/channels" "$CHANNEL"
CHANNEL_ID=$(echo "$CHANNEL" | python3 -c 'import sys,json; print(json.load(sys.stdin)["channelId"])' 2>/dev/null || true)
if [ -n "$CHANNEL_ID" ]; then
    pass "Create Channel ($CHANNEL_ID)"
else
    fail "Create Channel" "$CHANNEL"
fi

# ─── 7. List Channels ─────────────────────────────────────────
echo "── 7. List Channels ──"
CHANNELS=$(request GET "/servers/$SERVER_ID/channels")
print_resp "GET /servers/$SERVER_ID/channels" "$CHANNELS"
FOUND=$(echo "$CHANNELS" | python3 -c "
import sys,json; d=json.load(sys.stdin)
ids = [c['channelId'] for c in d['channels']]
assert '$CHANNEL_ID' in ids
print(len(d['channels']))
" 2>/dev/null || true)
if [ -n "$FOUND" ]; then
    pass "List Channels ($FOUND channels)"
else
    fail "List Channels" "$CHANNELS"
fi

# ─── 8. Send Message ──────────────────────────────────────────
echo "── 8. Send Message ──"
MSG=$(request POST "/channels/$CHANNEL_ID/messages" -d '{"text":"Hello World!"}')
print_resp "POST /channels/$CHANNEL_ID/messages" "$MSG"
MSG_ID=$(echo "$MSG" | python3 -c 'import sys,json; print(json.load(sys.stdin)["messageId"])' 2>/dev/null || true)
if [ -n "$MSG_ID" ]; then
    pass "Send Message ($MSG_ID)"
else
    fail "Send Message" "$MSG"
fi

# ─── 9. Get History ───────────────────────────────────────────
echo "── 9. Get History ──"
HISTORY=$(request GET "/channels/$CHANNEL_ID/messages?limit=10")
print_resp "GET /channels/$CHANNEL_ID/messages" "$HISTORY"
if echo "$HISTORY" | python3 -c "
import sys,json; d=json.load(sys.stdin)
msgs = d['messages']
assert len(msgs) > 0
m = msgs[0]
assert m['messageId'] == '$MSG_ID'
assert m['text'] == 'Hello World!'
assert isinstance(m['senderId'], str) and len(m['senderId']) == 36
assert m['isEdited'] == False
assert m['isDeleted'] == False
" 2>/dev/null; then
    pass "Get History"
else
    fail "Get History" "$HISTORY"
fi

# ─── 10. Edit Message ─────────────────────────────────────────
echo "── 10. Edit Message ──"
EDIT=$(request PUT "/channels/$CHANNEL_ID/messages/$MSG_ID" -d '{"text":"Edited!"}')
print_resp "PUT /channels/$CHANNEL_ID/messages/$MSG_ID" "$EDIT"
if echo "$EDIT" | python3 -c "
import sys,json; d=json.load(sys.stdin)
assert d['messageId'] == '$MSG_ID'
" 2>/dev/null; then
    pass "Edit Message"
else
    fail "Edit Message" "$EDIT"
fi

# verify edit in history
HIST2=$(request GET "/channels/$CHANNEL_ID/messages?limit=1")
print_resp "GET /channels/$CHANNEL_ID/messages (after edit)" "$HIST2"
if echo "$HIST2" | python3 -c "
import sys,json; d=json.load(sys.stdin)
m = d['messages'][0]
assert m['text'] == 'Edited!'
assert m['isEdited'] == True
" 2>/dev/null; then
    pass "Verify edit in history"
else
    fail "Verify edit in history" "$HIST2"
fi

# ─── 11. Read State ───────────────────────────────────────────
echo "── 11. Read State ──"
# initial GET (no state yet — skip validation)
RS0=$(request GET "/channels/$CHANNEL_ID/read-state")
print_resp "GET /channels/$CHANNEL_ID/read-state (initial)" "$RS0"

# PUT read state
RS=$(request PUT "/channels/$CHANNEL_ID/read-state" -d "{\"lastReadMessageId\":\"$MSG_ID\"}")
print_resp "PUT /channels/$CHANNEL_ID/read-state" "$RS"
if echo "$RS" | python3 -c "
import sys,json; d=json.load(sys.stdin)
assert d['channelId'] == '$CHANNEL_ID'
assert d['lastReadMessageId'] == '$MSG_ID'
" 2>/dev/null; then
    pass "Update Read State"
else
    fail "Update Read State" "$RS"
fi

# GET read state back
RS2=$(request GET "/channels/$CHANNEL_ID/read-state")
print_resp "GET /channels/$CHANNEL_ID/read-state (after update)" "$RS2"
if echo "$RS2" | python3 -c "
import sys,json; d=json.load(sys.stdin)
assert d['channelId'] == '$CHANNEL_ID'
assert d['lastReadMessageId'] == '$MSG_ID'
" 2>/dev/null; then
    pass "Get Read State"
else
    fail "Get Read State" "$RS2"
fi

# ─── 12. Delete Message ───────────────────────────────────────
echo "── 12. Delete Message ──"
DEL=$(request DELETE "/channels/$CHANNEL_ID/messages/$MSG_ID")
print_resp "DELETE /channels/$CHANNEL_ID/messages/$MSG_ID" "$DEL"
if echo "$DEL" | python3 -c '
import sys,json; d=json.load(sys.stdin)
assert d["status"] == "deleted"
' 2>/dev/null; then
    pass "Delete Message"
else
    fail "Delete Message" "$DEL"
fi

# verify delete in history
HIST3=$(request GET "/channels/$CHANNEL_ID/messages?limit=1")
print_resp "GET /channels/$CHANNEL_ID/messages (after delete)" "$HIST3"
if echo "$HIST3" | python3 -c "
import sys,json; d=json.load(sys.stdin)
m = d['messages'][0]
assert m['isDeleted'] == True
assert m['text'] == ''
" 2>/dev/null; then
    pass "Verify delete in history"
else
    fail "Verify delete in history" "$HIST3"
fi

# ─── 13. WebSocket Push Events (with mediaUrls & replyToMessageId) ──
echo "── 13. WebSocket Push Events ──"
WS_RESULT=$(BASE="$BASE" TOKEN="$TOKEN" CID="$CHANNEL_ID" SID="$SERVER_ID" python3 << 'PYEOF' 2>&1
import json, socket, os, time, base64
from urllib.request import Request, urlopen

BASE = os.environ["BASE"]
TOKEN = os.environ["TOKEN"]
CID = os.environ["CID"]
SID = os.environ["SID"]

def http_call(method, path, data=None):
    h = {"Content-Type": "application/json", "Authorization": f"Bearer {TOKEN}"}
    body = json.dumps(data).encode() if data else None
    r = Request(f"{BASE}{path}", data=body, headers=h, method=method)
    return json.loads(urlopen(r).read())

host = BASE.split("://")[1].split(":")[0]
key = base64.b64encode(os.urandom(16)).decode()
sock = socket.create_connection((host, 80))
sock.sendall(
    f"GET /chat?token={TOKEN} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n".encode()
)
resp = b""
while b"\r\n\r\n" not in resp:
    resp += sock.recv(4096)
if b"101" not in resp:
    print("WS_HANDSHAKE_FAIL")
    sock.close()
    exit(1)

sock.settimeout(5)
try:
    f = sock.recv(4096)
    if not f or b"connected" not in f:
        print("NO_CONNECTED_MSG")
        sock.close()
        exit(1)
    l = f[1] & 0x7F
    connected = f[2:2+l].decode()
    print(f"WS << {connected}")
except:
    print("NO_CONNECTED_MSG")
    sock.close()
    exit(1)

# send a message with mediaUrls and replyToMessageId
msg = http_call("POST", f"/channels/{CID}/messages", {
    "text": "WS test with media",
    "mediaUrls": ["https://example.com/img1.jpg", "https://example.com/img2.jpg"]
})
mid = msg["messageId"]
createdAt = msg["createdAt"]
print(f"REST POST /channels/{CID}/messages -> messageId={mid}, createdAt={createdAt}")

time.sleep(1.5)

try:
    all_data = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        all_data += chunk
        try:
            p = all_data.decode("utf-8", errors="replace")
            if '{"type":"new_message"' in p:
                d = json.loads(p[p.index('{'):p.rindex('}')+1])
                print(f"WS << {json.dumps(d)}")
                msg_data = d.get("message", {})
                assert msg_data.get("messageId") == mid, f"messageId mismatch: {msg_data.get('messageId')} != {mid}"
                assert msg_data.get("text") == "WS test with media", f"text mismatch"
                assert msg_data.get("mediaUrls") == ["https://example.com/img1.jpg", "https://example.com/img2.jpg"], f"mediaUrls mismatch"
                assert msg_data.get("replyToMessageId") is None, f"replyToMessageId should be null"
                assert d.get("serverId") == SID, f"serverId mismatch"
                print("WS_PASS")
                break
        except:
            continue
        if len(all_data) > 8192:
            print("WS_DATA_TOO_LARGE")
            break
except Exception as e:
    print(f"WS_ERR: {e}")

sock.close()
PYEOF
)
echo "$WS_RESULT"
if echo "$WS_RESULT" | grep -q "WS_PASS"; then
    pass "WebSocket push events (mediaUrls + replyToMessageId)"
else
    fail "WebSocket push events" "$WS_RESULT"
fi

# ─── Summary ──────────────────────────────────────────────────
echo ""
echo "──────────────────────────────────────"
echo "Results: $PASSED passed, $FAILED failed"
if [ "$FAILED" -eq 0 ]; then
    green "ALL TESTS PASSED"
else
    red "SOME TESTS FAILED"
    exit 1
fi
