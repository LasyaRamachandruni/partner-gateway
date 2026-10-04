"""A simulated fleet of connected vehicles.

Each car has a state (locked, battery, charging, location, odometer) and a
connection that comes and goes. A command sent to a car:

- runs after a short, variable delay when the car is online;
- waits in a queue when the car is offline, and runs when the car reconnects;
- expires if the car doesn't reconnect within `command_ttl_s`;
- can legitimately fail (START_CHARGING on a car that isn't plugged in).

When a command finishes, the fleet publishes the result to `vehicle-command-results`.
Commands are deduplicated by id, so a retried SendCommand never runs twice on the car.

`time_scale` shrinks every delay, so tests and the sandbox run fast with the same behavior.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone

from ..bus.base import Bus
from ..obs import tracing

RESULTS_TOPIC = "vehicle-command-results"
TELEMETRY_TOPIC = "vehicle-telemetry"
COMMANDS = ("LOCK", "UNLOCK", "START_CHARGING", "STOP_CHARGING", "HONK_AND_FLASH")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


@dataclass
class PendingCommand:
    command_id: str
    vin: str
    type: str
    partner_id: str
    trace: dict
    queued_at: float


class Fleet:
    def __init__(self, bus: Bus, vins: list[str] | None = None, *, seed: int = 0, time_scale: float = 1.0,
                 command_ttl_s: float = 60.0, latency_s: tuple[float, float] = (0.2, 1.5)):
        self.bus = bus
        self.rng = random.Random(seed)
        self.time_scale = time_scale
        self.command_ttl_s = command_ttl_s
        self.latency_s = latency_s
        vins = vins or [f"1FTFW1E{seed:02d}{i:08d}" for i in range(20)]
        self.vehicles: dict[str, Vehicle] = {}
        for vin in vins:
            self.vehicles[vin] = Vehicle(vin, plugged_in=self.rng.random() < 0.5,
                                         battery_percent=self.rng.randint(15, 95),
                                         odometer_km=round(self.rng.uniform(1000, 80000), 1),
                                         latitude=37.44 + self.rng.uniform(-0.2, 0.2),
                                         longitude=-122.14 + self.rng.uniform(-0.2, 0.2))
        self.seen: set[str] = set()  # command ids ever accepted: duplicates are ignored
        self.pending: dict[str, list[PendingCommand]] = {v: [] for v in self.vehicles}
        self.executed: list[str] = []
        self._tasks: set[asyncio.Task] = set()

    def get(self, vin: str) -> Vehicle | None:
        return self.vehicles.get(vin)

    def accept(self, command_id: str, vin: str, type_: str, partner_id: str, trace: dict) -> bool:
        """Returns False if this command id was already accepted (a retry)."""
        if command_id in self.seen:
            return False
        self.seen.add(command_id)
        cmd = PendingCommand(command_id, vin, type_, partner_id, trace, asyncio.get_running_loop().time())
        if self.vehicles[vin].online:
            self._spawn(self._run(cmd))
        else:
            self.pending[vin].append(cmd)
            self._spawn(self._expire_later(cmd))
        return True

    def _spawn(self, coro) -> None:
        t = asyncio.create_task(coro)
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    async def _run(self, cmd: PendingCommand) -> None:
        await asyncio.sleep(self.rng.uniform(*self.latency_s) * self.time_scale)
        v = self.vehicles[cmd.vin]
        status, reason = "SUCCEEDED", None
        if cmd.type == "LOCK":
            v.locked = True
        elif cmd.type == "UNLOCK":
            v.locked = False
        elif cmd.type == "START_CHARGING":
            if not v.plugged_in:
                status, reason = "FAILED", "vehicle is not plugged in"
            elif v.battery_percent >= 100:
                status, reason = "FAILED", "battery is full"
            else:
                v.charging = True
        elif cmd.type == "STOP_CHARGING":
            v.charging = False
        v.reported_at = now_iso()
        self.executed.append(cmd.command_id)
        await self._publish(cmd, status, reason)

    async def _expire_later(self, cmd: PendingCommand) -> None:
        await asyncio.sleep(self.command_ttl_s * self.time_scale)
        queue = self.pending.get(cmd.vin, [])
        if cmd in queue:
            queue.remove(cmd)
            await self._publish(cmd, "EXPIRED", f"vehicle offline for more than {self.command_ttl_s:.0f}s")

    async def _publish(self, cmd: PendingCommand, status: str, reason: str | None) -> None:
        with tracing.tracer("vehicle-service").start_as_current_span(
                "vehicle.command.result", context=tracing.extract(cmd.trace),
                attributes={"vin": cmd.vin, "command.status": status}):
            await self.bus.publish(RESULTS_TOPIC, {
                "command_id": cmd.command_id, "vin": cmd.vin, "type": cmd.type, "partner_id": cmd.partner_id,
                "status": status, "reason": reason, "completed_at": now_iso()}, tracing.inject())

    def set_online(self, vin: str, online: bool) -> None:
        """Connectivity changes; a reconnecting car runs its queued commands in order."""
        v = self.vehicles[vin]
        v.online = online
        if online:
            queued, self.pending[vin] = self.pending[vin], []
            for cmd in queued:
                self._spawn(self._run(cmd))

    async def publish_telemetry(self) -> int:
        n = 0
        for v in self.vehicles.values():
            if v.online:
                if v.charging:
                    v.battery_percent = min(100, v.battery_percent + 1)
                v.reported_at = now_iso()
                await self.bus.publish(TELEMETRY_TOPIC, {"vin": v.vin, "battery_percent": v.battery_percent,
                                                         "locked": v.locked, "charging": v.charging,
                                                         "reported_at": v.reported_at})
                n += 1
        return n

    async def close(self) -> None:
        for t in list(self._tasks):
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
