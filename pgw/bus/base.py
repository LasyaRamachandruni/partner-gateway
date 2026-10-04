"""Publish/subscribe messaging between services.

Topics used:
  vehicle-command-results   the vehicle service reports how each command ended
  vehicle-telemetry         periodic state reports from cars

Delivery is **at least once**. A subscriber can see the same message more than
once (a redelivery after a timeout, say), and it can see messages out of order.
Every consumer here is idempotent: applying a message twice has the same effect as
applying it once. A message whose handler keeps failing is retried with backoff,
then moved to a dead-letter topic (`<topic>.dead-letter`) for inspection, so
one bad message can't block the rest.

Trace context travels in message attributes (W3C `traceparent`), so a single trace
follows a command from the partner's request, through the vehicle service and the
event, to the webhook the partner receives.

Two implementations share this interface: `InMemoryBus` for tests and the sandbox,
and `GcpPubSubBus` for Google Cloud Pub/Sub (or its local emulator).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Awaitable, Callable, Protocol


@dataclass
class Message:
    message_id: str
    topic: str
    data: dict
    attributes: dict[str, str] = field(default_factory=dict)
    delivery_attempt: int = 1


Handler = Callable[[Message], Awaitable[None]]


class Bus(Protocol):
    async def publish(self, topic: str, data: dict, attributes: dict[str, str] | None = None) -> str: ...

    async def subscribe(self, topic: str, subscription: str, handler: Handler) -> None: ...

    async def close(self) -> None: ...
