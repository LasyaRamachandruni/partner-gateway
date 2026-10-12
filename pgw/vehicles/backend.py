"""What the vehicle service needs from "the cars".

Two implementations:

- `Fleet` (pgw/vehicles/fleet.py): simulated cars with connectivity, latency and failures.
- `SmartcarBackend` (pgw/vehicles/smartcar.py): vehicles reached through the Smartcar API,
  including Smartcar's own Vehicle Simulator.

Choose with `PGW_VEHICLE_BACKEND=simulated|smartcar`. Either way, a command is
accepted at once and its result is published to `vehicle-command-results` when
the car has finished, so the gateway can't tell the backends apart.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol

from ..bus.base import Bus
from ..obs import tracing

RESULTS_TOPIC = "vehicle-command-results"
TELEMETRY_TOPIC = "vehicle-telemetry"
COMMANDS = ("LOCK", "UNLOCK", "START_CHARGING", "STOP_CHARGING", "HONK_AND_FLASH")


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class Vehicle:
    vin: str
    online: bool = True
    locked: bool = True
    plugged_in: bool = False
    charging: bool = False
    battery_percent: int = 70
    odometer_km: float = 12000.0
    latitude: float = 37.4419
    longitude: float = -122.1430
    reported_at: str = field(default_factory=now_iso)


class VehicleBackend(Protocol):
    name: str

    async def state(self, vin: str) -> Vehicle | None:
        """Current state, or None if the backend has no such vehicle."""
        ...

    def accept(self, command_id: str, vin: str, type_: str, partner_id: str, trace: dict[str, str]) -> bool:
        """Start a command. False if this command id was already accepted (a retry)."""
        ...

    async def publish_telemetry(self) -> int: ...

    async def close(self) -> None: ...


async def publish_result(
    bus: Bus,
    *,
    command_id: str,
    vin: str,
    type_: str,
    partner_id: str,
    trace: dict[str, str],
    status: str,
    reason: str | None,
) -> None:
    with tracing.tracer("vehicle-service").start_as_current_span(
        "vehicle.command.result", context=tracing.extract(trace), attributes={"vin": vin, "command.status": status}
    ):
        await bus.publish(
            RESULTS_TOPIC,
            {
                "command_id": command_id,
                "vin": vin,
                "type": type_,
                "partner_id": partner_id,
                "status": status,
                "reason": reason,
                "completed_at": now_iso(),
            },
            tracing.inject(),
        )
