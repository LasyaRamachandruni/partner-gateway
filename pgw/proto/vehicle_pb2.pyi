from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CommandType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    COMMAND_TYPE_UNSPECIFIED: _ClassVar[CommandType]
    LOCK: _ClassVar[CommandType]
    UNLOCK: _ClassVar[CommandType]
    START_CHARGING: _ClassVar[CommandType]
    STOP_CHARGING: _ClassVar[CommandType]
    HONK_AND_FLASH: _ClassVar[CommandType]
COMMAND_TYPE_UNSPECIFIED: CommandType
LOCK: CommandType
UNLOCK: CommandType
START_CHARGING: CommandType
STOP_CHARGING: CommandType
HONK_AND_FLASH: CommandType

class GetVehicleStateRequest(_message.Message):
    __slots__ = ("vin",)
    VIN_FIELD_NUMBER: _ClassVar[int]
    vin: str
    def __init__(self, vin: _Optional[str] = ...) -> None: ...

class VehicleState(_message.Message):
    __slots__ = ("vin", "online", "locked", "battery_percent", "odometer_km", "latitude", "longitude", "reported_at")
    VIN_FIELD_NUMBER: _ClassVar[int]
    ONLINE_FIELD_NUMBER: _ClassVar[int]
    LOCKED_FIELD_NUMBER: _ClassVar[int]
    BATTERY_PERCENT_FIELD_NUMBER: _ClassVar[int]
    ODOMETER_KM_FIELD_NUMBER: _ClassVar[int]
    LATITUDE_FIELD_NUMBER: _ClassVar[int]
    LONGITUDE_FIELD_NUMBER: _ClassVar[int]
    REPORTED_AT_FIELD_NUMBER: _ClassVar[int]
    vin: str
    online: bool
    locked: bool
    battery_percent: int
    odometer_km: float
    latitude: float
    longitude: float
    reported_at: str
    def __init__(self, vin: _Optional[str] = ..., online: _Optional[bool] = ..., locked: _Optional[bool] = ..., battery_percent: _Optional[int] = ..., odometer_km: _Optional[float] = ..., latitude: _Optional[float] = ..., longitude: _Optional[float] = ..., reported_at: _Optional[str] = ...) -> None: ...

class SendCommandRequest(_message.Message):
    __slots__ = ("command_id", "vin", "type", "partner_id")
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    VIN_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    PARTNER_ID_FIELD_NUMBER: _ClassVar[int]
    command_id: str
    vin: str
    type: CommandType
    partner_id: str
    def __init__(self, command_id: _Optional[str] = ..., vin: _Optional[str] = ..., type: _Optional[_Union[CommandType, str]] = ..., partner_id: _Optional[str] = ...) -> None: ...

class SendCommandResponse(_message.Message):
    __slots__ = ("command_id", "accepted", "duplicate")
    COMMAND_ID_FIELD_NUMBER: _ClassVar[int]
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    DUPLICATE_FIELD_NUMBER: _ClassVar[int]
    command_id: str
    accepted: bool
    duplicate: bool
    def __init__(self, command_id: _Optional[str] = ..., accepted: _Optional[bool] = ..., duplicate: _Optional[bool] = ...) -> None: ...
