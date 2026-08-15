"""Deprecated compatibility exports.

Tasklet transport now lives in ``app.adapters.tasklet``. Import from that
module in new code.
"""

from .adapters.tasklet import TaskletAdapter, compile_tasklet_message, parse_tasklet_event
from .adapters.base import GatewayError

TaskletError = GatewayError

__all__ = ["TaskletAdapter", "TaskletError", "compile_tasklet_message", "parse_tasklet_event"]
