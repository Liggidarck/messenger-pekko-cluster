# Messenger Cluster

A real-time messaging platform built on [Apache Pekko](https://pekko.apache.org/) cluster — a Discord/Telegram-like messenger with REST API, WebSocket push, JWT auth, and event-sourced chat persistence.

## Architecture

Three microservices form a single Pekko cluster. They discover each other via Kubernetes API and communicate over Pekko remoting (Artery).

```
┌──────────────────────────────────────────────┐
│              Pekko Cluster                   │
│                                              │
│  ┌──────────────┐  ┌──────────────────────┐  │
│  │  Gateway     │  │   Auth               │  │
│  │  :8080       │◄─┤   (no HTTP)          │  │
│  │  REST + WS   │  │   PostgreSQL         │  │
│  └──────┬───────┘  └──────────────────────┘  │
│         │                                    │
│         ▼                                    │
│  ┌──────────────────────┐                    │
│  │  Chat Core           │                    │
│  │  (no HTTP)           │                    │
│  │  Cassandra           │                    │
│  │  Event Sourcing      │                    │
│  └──────────────────────┘                    │
└──────────────────────────────────────────────┘
```

| Service | Role | HTTP Port | Persistence |
|---------|------|-----------|-------------|
| `messenger-api-gateway` | REST + WebSocket entry point | 8080 | None |
| `messenger-auth` | Registration, login, JWT | — (internal) | PostgreSQL |
| `messenger-chat-core` | Messaging, channels, read state | — (internal) | Cassandra |

**Cluster communication:**

- Services find each other via Pekko Cluster Bootstrap using Kubernetes API discovery.
- The Gateway routes requests to Auth (via Receptionist) and Chat Core (via Cluster Sharding).
- WebSocket connections are managed per-user through sharded `UserEntityActor` in Chat Core.
- All internal communication uses Pekko serialization (Jackson CBOR).

## Quick Start (local dev with minikube)

```bash
# 1. Start minikube
minikube start --cpus=4 --memory=8g
minikube addons enable ingress
eval $(minikube docker-env)
export DOCKER_BUILDKIT=1

# 2. Build all service images via SBT
sbt pubAll

# 3. Install the Helm chart
helm install messenger ./messenger-chart -f ./messenger-chart/values-dev.yaml

# 4. Add host entry
echo "$(minikube ip) messenger.local" | sudo tee -a /etc/hosts

# 5. Wait for pods
kubectl get pods -w
# All pods should reach Running + 1/1

# 6. Open in browser
open http://messenger.local
```

See [messenger-chart/README.md](messenger-chart/README.md) for detailed dev workflow.

## REST API

All endpoints are behind `messenger-api-gateway` on port 8080 (or `messenger.local` via ingress in dev).

Authentication is via **Bearer JWT token** obtained from `POST /login`.

Error responses follow the format: `{"error": "message"}` with appropriate HTTP status.

### Authentication

#### `POST /register`
Create a new user.

**Request body:**
```json
{
  "name": "string",
  "lastName": "string",
  "email": "string",
  "password": "string"
}
```

**Response** `201 Created`:
```json
{
  "user": {
    "id": "uuid",
    "name": "string",
    "lastName": "string",
    "email": "string"
  }
}
```

---

#### `POST /login`
Authenticate and receive a JWT token.

**Request body:**
```json
{
  "email": "string",
  "password": "string"
}
```

**Response** `200 OK`:
```json
{
  "token": "jwt-string"
}
```

**Error** `401 Unauthorized`:
```json
{
  "error": "Invalid credentials"
}
```

---

#### `POST /me`
Get the current user's profile (from the JWT token).

**Headers:** `Authorization: Bearer <token>`

**Response** `200 OK`:
```json
{
  "user": {
    "id": "uuid",
    "name": "string",
    "lastName": "string",
    "email": "string"
  }
}
```

---

#### `GET /users/{userId}`
Get a user by UUID.

**Headers:** `Authorization: Bearer <token>`

**Response** `200 OK` — same shape as `/me`.

---

#### `GET /users?email={email}`
Look up a user by email.

**Headers:** `Authorization: Bearer <token>`

**Response** `200 OK`:
```json
{
  "user": {
    "id": "uuid",
    "name": "string",
    "lastName": "string",
    "email": "string"
  }
}
```

**Error** `400 Bad Request` — email parameter is missing.

### Servers

#### `POST /servers`
Create a new server. The creator automatically becomes a member.

**Headers:** `Authorization: Bearer <token>`

**Request body:**
```json
{
  "name": "string"
}
```

**Response** `201 Created`:
```json
{
  "serverId": "uuid"
}
```

---

#### `GET /servers`
List servers the current user is a member of.

**Headers:** `Authorization: Bearer <token>`

**Response** `200 OK`:
```json
{
  "servers": [
    {
      "serverId": "uuid",
      "serverName": "string",
      "ownerId": "uuid",
      "createdAt": 1234567890000
    }
  ]
}
```

---

#### `GET /servers/{serverId}`
Get server profile.

**Headers:** `Authorization: Bearer <token>`

**Response** `200 OK`:
```json
{
  "serverId": "uuid",
  "name": "string",
  "ownerId": "uuid",
  "createdAt": 1234567890000,
  "memberCount": 5,
  "channelCount": 3
}
```

**Error** `404 Not Found`.

---

#### `GET /servers/{serverId}/members`
List server members.

**Headers:** `Authorization: Bearer <token>`

**Response** `200 OK`:
```json
["userId1", "userId2", "userId3"]
```

---

#### `POST /servers/{serverId}/members`
Add a member to the server.

**Headers:** `Authorization: Bearer <token>`

**Request body** (one of):
```json
{ "userId": "uuid" }
```
or
```json
{ "email": "user@example.com" }
```

**Response** `200 OK`:
```json
{ "status": "success" }
```

**Errors:**
- `400` — missing userId/email, or user is already a member
- `404` — user not found

### Channels

#### `GET /servers/{serverId}/channels`
List channels in a server.

**Headers:** `Authorization: Bearer <token>`

**Response** `200 OK`:
```json
{
  "serverId": "uuid",
  "channels": [
    {
      "channelId": "uuid",
      "channelName": "string"
    }
  ]
}
```

---

#### `POST /servers/{serverId}/channels`
Create a channel in a server.

**Headers:** `Authorization: Bearer <token>`

**Request body:**
```json
{ "name": "string" }
```

**Response** `200 OK`:
```json
{
  "serverId": "uuid",
  "channelId": "uuid",
  "channelName": "string"
}
```

### Messages

#### `GET /channels/{channelId}/messages?limit=50&beforeMessageId={uuid}`
Get message history. Messages are returned newest-first.

**Headers:** `Authorization: Bearer <token>`

**Query params:**
- `limit` — number of messages (default: 50)
- `beforeMessageId` — UUID of the oldest loaded message (for cursor pagination)

**Response** `200 OK`:
```json
{
  "channelId": "uuid",
  "messages": [
    {
      "messageId": "uuid",
      "senderId": "uuid",
      "text": "string",
      "createdAt": 1234567890000,
      "mediaUrls": ["https://..."],
      "replyToMessageId": "uuid | null",
      "isEdited": false,
      "editedAt": 1234567890000,
      "isDeleted": false
    }
  ],
  "hasMore": true
}
```

---

#### `POST /channels/{channelId}/messages`
Send a message.

**Headers:** `Authorization: Bearer <token>`

**Request body:**
```json
{
  "text": "string",
  "mediaUrls": ["https://..."],
  "replyToMessageId": "uuid"
}
```
`mediaUrls` and `replyToMessageId` are optional.

**Response** `201 Created`:
```json
{
  "messageId": "uuid",
  "createdAt": 1234567890000
}
```

---

#### `PUT /channels/{channelId}/messages/{messageId}`
Edit a message (soft edit).

**Headers:** `Authorization: Bearer <token>`

**Request body:**
```json
{ "text": "new text" }
```

**Response** `200 OK`:
```json
{
  "messageId": "uuid",
  "editedAt": 1234567890000
}
```

---

#### `DELETE /channels/{channelId}/messages/{messageId}`
Soft-delete a message (`text` is cleared, `isDeleted` set to `true`).

**Headers:** `Authorization: Bearer <token>`

**Response** `200 OK`:
```json
{ "status": "deleted" }
```

### Read State

#### `GET /channels/{channelId}/read-state`
Get the user's last read message in a channel and the unread count.

**Headers:** `Authorization: Bearer <token>`

**Response** `200 OK`:
```json
{
  "channelId": "uuid",
  "lastReadMessageId": "uuid | null",
  "unreadCount": 5
}
```
- `lastReadMessageId` is `null` if no read state has been set.
- `unreadCount` is capped at 99 (shows `99+` when >= 100).

---

#### `PUT /channels/{channelId}/read-state`
Update the last read message.

**Headers:** `Authorization: Bearer <token>`

**Request body:**
```json
{ "lastReadMessageId": "uuid" }
```

**Response** `200 OK`:
```json
{
  "channelId": "uuid",
  "lastReadMessageId": "uuid",
  "unreadCount": 0
}
```

## WebSocket API

Connect to the real-time event stream:

```
GET /chat?token={jwt_token}
```

On successful connection, the server sends a plain string:
```
connected
```

### Client → Server

#### Typing indicator
```json
{
  "type": "typing",
  "channelId": "uuid"
}
```

#### Ping (keep-alive)
```json
{
  "type": "ping"
}
```

### Server → Client

All events are JSON with a `type` field.

#### New message
```json
{
  "type": "new_message",
  "serverId": "uuid",
  "channelId": "uuid",
  "message": {
    "messageId": "uuid",
    "senderId": "uuid",
    "text": "string",
    "createdAt": 1234567890000,
    "mediaUrls": ["https://..."],
    "replyToMessageId": "uuid | null"
  }
}
```

#### Message edited
```json
{
  "type": "message_edited",
  "serverId": "uuid",
  "channelId": "uuid",
  "messageId": "uuid",
  "text": "string",
  "editedAt": 1234567890000
}
```

#### Message deleted
```json
{
  "type": "message_deleted",
  "serverId": "uuid",
  "channelId": "uuid",
  "messageId": "uuid"
}
```

#### Server created
```json
{
  "type": "server_created",
  "serverId": "uuid",
  "serverName": "string"
}
```

#### Channel created
```json
{
  "type": "channel_created",
  "serverId": "uuid",
  "channelId": "uuid",
  "channelName": "string"
}
```

#### User typing
```json
{
  "type": "user_typing",
  "channelId": "uuid",
  "userId": "uuid"
}
```

## Testing

Run integration tests after deployment:

```bash
# All pods must be Ready
kubectl get pods

# Full-cycle integration test (register, server, channel, message, read state)
bash test/integration_test.sh

# Cross-user WebSocket push event tests
bash test/complex_test.sh
```

Both scripts exit with **0** on success and print `ALL TESTS PASSED` / `ALL COMPLEX TESTS PASSED`.

## Project Structure

```
├── messenger-api-gateway/     # REST + WebSocket entry point (Pekko HTTP)
├── messenger-auth/            # Auth service (PostgreSQL, JWT, Flyway)
├── messenger-chat-core/       # Chat logic (Cassandra, Event Sourcing)
├── messenger-shared/          # Shared protocols, messages, serialization
├── messenger-chart/           # Helm chart for Kubernetes deployment
│   └── templates/
│       ├── app/               # Service deployments
│       └── infrastructure/    # Cassandra, PostgreSQL, RBAC
└── test/                      # Integration test scripts
```

## Configuration

Each service has an `application.conf` on the classpath. Common Pekko cluster settings are factored into `pekko-cluster-base.conf` in `messenger-shared` and included by all services.

Key environment variables:

| Variable | Used By | Default               |
|----------|---------|-----------------------|
| `K8S_POD_IP` | All | `127.0.0.1`           |
| `JWT_SECRET` | auth | `secret-key-for-only` |
| `JWT_LIFE_TIME` | auth | `86400000` (24h)      |
| `DB_HOST` | auth | `127.0.0.1`           |
| `DB_NAME` | auth | *(required)*          |
| `DB_USER` | auth | `messenger`           |
| `DB_PASSWORD` | auth | `password`            |
| `CASSANDRA_CONTACT_POINTS` | chat-core | `127.0.0.1:9042`      |
