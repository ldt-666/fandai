# fandai — multi-protocol Tasklet AI gateway

`fandai` exposes text-only compatibility endpoints backed by Tasklet AI:

- `POST /v1/chat/completions` — OpenAI Chat Completions
- `POST /v1/responses` — OpenAI Responses text subset
- `POST /v1/messages` — Anthropic Messages text subset
- `GET /healthz` — liveness
- `GET /readyz` — configuration/router readiness

All three inference endpoints use the same configured adapter. Tasklet messages
are submitted over HTTP and the resulting agent blocks are synchronized over an
authenticated WebSocket. Tokens are stored only in `config.yaml`, never logged
or returned.

## Configuration

The preferred v2 configuration is:

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
    aliases: ["tasklet"]
```

Copy and edit:

```powershell
Copy-Item config.example.yaml config.yaml
notepad config.yaml
```

The current legacy `tasklet:` configuration is still accepted and translated
in memory, with a migration warning. `config.yaml` is gitignored.

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

## Docker / Ubuntu 22.04

```bash
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-plugin
git clone <your-repository-url> fandai
cd fandai
cp config.example.yaml config.yaml
$EDITOR config.yaml
docker compose up -d
docker compose ps
docker compose logs -f fandai
```

The image runs as a non-root user. `config.yaml` is mounted read-only and is
not copied into the image. Compose health checks `/readyz`; an upstream Tasklet
outage does not cause a container restart loop.

## Codex / OpenAI clients

Set the base URL to `http://server:8000/v1` and use any gateway key accepted
by the client. Ensure the client uses Chat Completions or Responses as
configured; newer clients may require `/v1/responses`.

```bash
export OPENAI_BASE_URL=http://server:8000/v1
export OPENAI_API_KEY=gateway-key
```
