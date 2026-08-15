"""Versioned gateway configuration and model routing."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .adapters import AdapterRoute, TaskletAdapter

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TaskletConfig:
    api_url: str
    token: str
    agent_id: str
    workspace_id: str
    timezone: str = "Asia/Singapore"


@dataclass(frozen=True)
class GatewayConfig:
    adapters: dict[str, TaskletConfig]
    models: dict[str, AdapterRoute]
    aliases: dict[str, AdapterRoute]
    legacy: bool = False

    def resolve_model(self, model: str) -> AdapterRoute:
        route = self.aliases.get(model)
        if route is None:
            raise KeyError(model)
        return route


def _config_path() -> Path:
    value = os.getenv("FANDAI_CONFIG")
    if value:
        return Path(value).expanduser().resolve()
    return Path(__file__).resolve().parent.parent / "config.yaml"


def _redacted_tasklet(value: TaskletConfig) -> dict[str, Any]:
    return {
        "api_url": value.api_url,
        "token": "***",
        "agent_id": value.agent_id,
        "workspace_id": value.workspace_id,
        "timezone": value.timezone,
    }


def _required(section: dict[str, Any], keys: tuple[str, ...], label: str) -> None:
    missing = [key for key in keys if not section.get(key)]
    if missing:
        raise ValueError(f"Missing required {label} keys: {', '.join(missing)}")


def load_config(path: Path | None = None) -> GatewayConfig:
    config_path = path or _config_path()
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError("Configuration root must be a mapping")

    if "tasklet" in data and ("adapters" in data or "models" in data):
        raise ValueError("Use either legacy tasklet config or v2 adapters/models config, not both")
    if "tasklet" in data:
        tasklet = data["tasklet"]
        if not isinstance(tasklet, dict):
            raise ValueError("tasklet configuration must be a mapping")
        _required(tasklet, ("api_url", "token", "agent_id", "workspace_id"), "tasklet")
        adapter_cfg = TaskletConfig(
            api_url=str(tasklet["api_url"]), token=str(tasklet["token"]),
            agent_id=str(tasklet["agent_id"]), workspace_id=str(tasklet["workspace_id"]),
            timezone=str(tasklet.get("timezone") or "Asia/Singapore"),
        )
        route = AdapterRoute("tasklet", "tasklet", adapter_cfg.agent_id, adapter_cfg.workspace_id, adapter_cfg.timezone, ("tasklet",))
        logger.warning("Using legacy tasklet config; migrate to adapters/models v2 when convenient")
        logger.info("Loaded config: %s", {"tasklet": _redacted_tasklet(adapter_cfg)})
        return GatewayConfig({"tasklet": adapter_cfg}, {"tasklet": route}, {"tasklet": route}, legacy=True)

    if not isinstance(data.get("adapters"), dict) or not isinstance(data.get("models"), dict):
        raise ValueError("Configuration requires adapters: and models: sections")

    adapters: dict[str, TaskletConfig] = {}
    for name, raw in data["adapters"].items():
        if not isinstance(raw, dict) or raw.get("type") != "tasklet":
            raise ValueError(f"Unsupported adapter {name!r}; only type: tasklet is available")
        _required(raw, ("api_url", "token"), f"adapter {name}")
        adapters[name] = TaskletConfig(
            api_url=str(raw["api_url"]), token=str(raw["token"]),
            agent_id="", workspace_id="", timezone="Asia/Singapore",
        )

    models: dict[str, AdapterRoute] = {}
    aliases: dict[str, AdapterRoute] = {}
    for name, raw in data["models"].items():
        if not isinstance(raw, dict):
            raise ValueError(f"Model route {name!r} must be a mapping")
        adapter_name = str(raw.get("adapter") or "")
        if adapter_name not in adapters:
            raise ValueError(f"Model route {name!r} references unknown adapter {adapter_name!r}")
        _required(raw, ("agent_id", "workspace_id"), f"model {name}")
        route = AdapterRoute(
            name=name, adapter=adapter_name, agent_id=str(raw["agent_id"]),
            workspace_id=str(raw["workspace_id"]), timezone=str(raw.get("timezone") or "Asia/Singapore"),
            aliases=tuple(str(alias) for alias in (raw.get("aliases") or [])),
        )
        models[name] = route
        for alias in (name, *route.aliases):
            if alias in aliases:
                raise ValueError(f"Duplicate model alias: {alias}")
            aliases[alias] = route

    logger.info("Loaded v2 config with adapters=%s models=%s", list(adapters), list(models))
    return GatewayConfig(adapters, models, aliases)


def build_adapters(config: GatewayConfig, client) -> dict[str, TaskletAdapter]:
    return {
        name: TaskletAdapter(client, api_url=adapter.api_url, token=adapter.token)
        for name, adapter in config.adapters.items()
    }
