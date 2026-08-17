# fandai — multi-protocol Tasklet AI gateway

`fandai` exposes text-only compatibility endpoints backed by Tasklet AI:

- `POST /v1/chat/completions` — OpenAI Chat Completions
- `POST /v1/responses` — OpenAI Responses text subset
- `POST /v1/messages` — Anthropic Messages text subset
- `GET /healthz` — liveness
- `GET /readyz` — configuration/router readiness

All three inference endpoints use the same configured adapter or account pool.
Tasklet messages are submitted over HTTP and the resulting agent blocks are
synchronized over an authenticated WebSocket. Tokens are stored only in
`config.yaml`, never logged or returned.

## Configuration

The preferred configuration uses the in-process Tasklet account pool:

```yaml
version: 2
accounts:
  - name: account1
    token: "YOUR_TOKEN_1"
    agent_id: "YOUR_AGENT_ID_1"
    workspace_id: "YOUR_WORKSPACE_ID_1"
  - name: account2
    token: "YOUR_TOKEN_2"
    agent_id: "YOUR_AGENT_ID_2"
    workspace_id: "YOUR_WORKSPACE_ID_2"
    timezone: "Asia/Singapore"
    # api_url: "https://api.tasklet.ai/api/sendChatMessage"
```

Clients continue to send `model: "tasklet"`. The gateway selects healthy
accounts round-robin and keeps the selected token, agent, workspace, and
WebSocket fixed for the full request. Each account serves at most one active
request because Tasklet synchronizes state per agent; separate accounts can run
in parallel.

Authentication failures and exhausted credits disable an account until the
process restarts. Temporary upstream/network failures put it into an in-memory
cooldown. A failure known to happen before Tasklet accepts the trigger can try
the next account. Once Tasklet may have accepted a trigger, the gateway does not
replay it on another account, which avoids duplicate work or combining two
accounts in one streamed reply. State is process-local; there is no database,
web dashboard, or account-management API.

`api_url` defaults to
`https://api.tasklet.ai/api/sendChatMessage`; `timezone` defaults to
`Asia/Singapore`. Account names must be unique, and `accounts:` cannot be mixed
with the single-account formats below.

The existing v2 single-account configuration remains supported:

```yaml
version: 2
adapters:
  tasklet:
    type: tasklet
    api_url: "https://api.tasklet.ai/api/sendChatMessage"
    token: "YOUR_TOKEN"
models:
  tasklet:
    adapter: tasklet
    agent_id: "YOUR_AGENT_ID"
    workspace_id: "YOUR_WORKSPACE_ID"
    timezone: "Asia/Singapore"
```

Copy and edit:

```powershell
Copy-Item config.example.yaml config.yaml
notepad config.yaml
```

The legacy `tasklet:` configuration is also still accepted and translated in
memory, with a migration warning. `config.yaml` is gitignored.

## Local run

```powershell
python -m pip install -r requirements.txt
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Check readiness:

```powershell
curl.exe http://localhost:8000/healthz
curl.exe http://localhost:8000/readyz
```

## API examples

### OpenAI Chat Completions

```powershell
curl.exe -N -X POST http://localhost:8000/v1/chat/completions `
  -H "Content-Type: application/json" `
  -d '{"model":"tasklet","messages":[{"role":"user","content":"你好，请介绍一下自己"}],"stream":true}'
```

### OpenAI Responses

```powershell
curl.exe -N -X POST http://localhost:8000/v1/responses `
  -H "Content-Type: application/json" `
  -d '{"model":"tasklet","input":"你好","stream":true}'
```

### Anthropic Messages / Claude Code compatibility

```powershell
curl.exe -N -X POST http://localhost:8000/v1/messages `
  -H "Content-Type: application/json" `
  -H "anthropic-version: 2023-06-01" `
  -H "x-api-key: gateway-key" `
  -d '{"model":"tasklet","max_tokens":1024,"messages":[{"role":"user","content":"你好"}],"stream":true}'
```

The Messages endpoint accepts ordinary Anthropic request headers. Because
Tasklet is text-only, Anthropic `tools` and `thinking` request parameters are
accepted but ignored; responses contain text only. Images, files, citations,
and tool-result content remain unsupported. Claude Code versions that allow a
custom Anthropic base URL can use `/v1`; verify the client version's
configuration before deployment.

## Supported boundaries

This release intentionally supports text only. Anthropic Messages accepts and
ignores `tools` and `thinking` so text-only clients can degrade gracefully. It
still rejects tool calls/tool results, image/audio/file content, stateful
Responses continuation, and background mode. Tasklet usage counts are
unavailable and therefore reported as zero.

## Real Tasklet verification

Do not put credentials in chat. Fill the local `config.yaml` directly, then
run:

```powershell
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
curl.exe -N -X POST http://localhost:8000/v1/chat/completions `
  -H "Content-Type: application/json" `
  -d '{"model":"tasklet","messages":[{"role":"user","content":"你好，请介绍一下自己"}],"stream":true}'
```

The adapter sends exactly:

```json
{
  "agentId": "...",
  "message": "...",
  "timezone": "Asia/Singapore",
  "fileIds": [],
  "workspaceId": "..."
}
```

with `Authorization: Bearer <token>`. Tasklet returns the accepted `agentId`
from this request; the assistant text is then read from `agent_content` blocks
on `wss://<api-host>/api/sync` using the same bearer token as the sync session.

## Client authentication

The three inference endpoints (`/v1/chat/completions`, `/v1/responses`,
`/v1/messages`) can be protected with a simple client API key, set via the
`FANDAI_API_KEY` environment variable. This is independent of the upstream
Tasklet tokens in `config.yaml` — it only controls who may call the gateway.

Clients may present the key in either header, for OpenAI/Codex and
Anthropic/Claude compatibility:

```
Authorization: Bearer YOUR_KEY
x-api-key: YOUR_KEY
```

A missing or incorrect key returns `401`. When `FANDAI_API_KEY` is unset or
empty, authentication is disabled and all requests pass through (the previous
open behaviour, convenient for local development). Health endpoints
(`/healthz`, `/readyz`) are never authenticated.

## Docker / Ubuntu 22.04 production deployment

Full flow for a public server behind nginx + HTTPS. The gateway binds to
`127.0.0.1:8000` only; nginx terminates TLS and reverse-proxies to it.

### 1. Install Docker

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo $VERSION_CODENAME) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
```

### 2. Upload the code

```bash
git clone <your-repository-url> fandai
cd fandai
```

### 3. Configure the Tasklet accounts

```bash
cp config.example.yaml config.yaml
$EDITOR config.yaml   # fill in real tokens / agent_id / workspace_id
```

### 4. Configure the client API key

```bash
cp .env.example .env
# Generate a strong secret and put it in .env as FANDAI_API_KEY:
openssl rand -hex 32
$EDITOR .env
```

### 5. Start the gateway

`docker compose` reads `.env` automatically and passes `FANDAI_API_KEY` into the
container. The port is bound to `127.0.0.1:8000` — not reachable from the
public internet.

```bash
docker compose up -d
docker compose ps
docker compose logs -f fandai
```

`config.yaml` is mounted read-only and is not copied into the image. The image
runs as a non-root user. Compose health-checks `/readyz`; an upstream Tasklet
outage does not cause a container restart loop.

### 6. Configure nginx + HTTPS

```bash
sudo apt-get install -y nginx
sudo cp nginx.conf.example /etc/nginx/sites-available/fandai
# Edit server_name and TLS certificate paths:
sudo $EDITOR /etc/nginx/sites-available/fandai
sudo ln -s /etc/nginx/sites-available/fandai /etc/nginx/sites-enabled/fandai
sudo rm -f /etc/nginx/sites-enabled/default

# Obtain a certificate (Let's Encrypt):
sudo apt-get install -y certbot python3-certbot-nginx
sudo certbot --nginx -d your-domain.example.com

sudo nginx -t && sudo systemctl reload nginx
```

The example config proxies all three endpoints to `127.0.0.1:8000`, disables
proxy buffering for SSE, sets generous timeouts, and forwards `Host`,
`X-Real-IP`, and `X-Forwarded-For`.

### 7. Test with curl

```bash
# Health (no key required)
curl -s https://your-domain.example.com/readyz

# Missing key -> 401
curl -s -o /dev/null -w "%{http_code}\n" -X POST \
  https://your-domain.example.com/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"tasklet","messages":[{"role":"user","content":"hi"}]}'

# Bearer key, streaming
curl -N -X POST https://your-domain.example.com/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_KEY" \
  -d '{"model":"tasklet","messages":[{"role":"user","content":"你好"}],"stream":true}'

# x-api-key (Anthropic Messages)
curl -N -X POST https://your-domain.example.com/v1/messages \
  -H "Content-Type: application/json" \
  -H "anthropic-version: 2023-06-01" \
  -H "x-api-key: YOUR_KEY" \
  -d '{"model":"tasklet","max_tokens":1024,"messages":[{"role":"user","content":"你好"}],"stream":true}'
```

## Codex / OpenAI clients

Set the base URL to `http://server:8000/v1` (or your HTTPS domain) and set the
gateway key you configured in `FANDAI_API_KEY`. Ensure the client uses Chat
Completions or Responses as configured; newer clients may require
`/v1/responses`.

```bash
export OPENAI_BASE_URL=https://your-domain.example.com/v1
export OPENAI_API_KEY=YOUR_KEY
```
