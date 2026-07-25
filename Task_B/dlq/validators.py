"""
Validation rules for UrbanPulse streams. Each validate_* function returns
(is_valid: bool, error_reason: str | None). Records failing validation are
routed to urbanpulse.dlq with the error_reason attached (Task B, problem 8).

Minimum 3 validation rules per the assignment; we implement rules across
all 4 streams so the DLQ demo has real, varied traffic.
"""


def validate_bus_gps(rec: dict):
    lat, lon = rec.get("lat"), rec.get("lon")
    if lat is None or lon is None:
        return False, "NULL_COORDINATES"
    if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        return False, "IMPOSSIBLE_GPS_COORDINATES"
    speed = rec.get("speed_kmh")
    if speed is None or speed < 0 or speed > 120:
        return False, "SPEED_OUT_OF_RANGE"
    occ = rec.get("occupancy_pct")
    if occ is None or not (0 <= occ <= 100):
        return False, "OCCUPANCY_OUT_OF_RANGE"
    if not rec.get("route_id") or not rec.get("bus_id"):
        return False, "MISSING_ID_FIELD"
    return True, None


def validate_air_quality(rec: dict):
    aqi = rec.get("aqi")
    if aqi is None:
        return False, "NULL_AQI"
    if aqi < 0 or aqi > 500:
        return False, "AQI_OUT_OF_RANGE"
    for field in ("pm25", "pm10", "no2"):
        val = rec.get(field)
        if val is None or val < 0:
            return False, f"NEGATIVE_OR_MISSING_{field.upper()}"
    if not rec.get("sensor_id") or not rec.get("zone"):
        return False, "MISSING_ID_FIELD"
    return True, None


def validate_traffic_signals(rec: dict):
    if rec.get("vehicle_count") is None or rec["vehicle_count"] < 0:
        return False, "NEGATIVE_VEHICLE_COUNT"
    wait = rec.get("avg_wait_sec")
    if wait is None or wait < 0 or wait > 600:
        return False, "WAIT_TIME_OUT_OF_RANGE"
    if rec.get("signal_phase") not in ("GREEN", "RED", "AMBER"):
        return False, "INVALID_SIGNAL_PHASE"
    if not rec.get("junction_id") or not rec.get("zone"):
        return False, "MISSING_ID_FIELD"
    return True, None


def validate_smart_meters(rec: dict):
    kwh = rec.get("kwh_reading")
    if kwh is None or kwh < 0:
        return False, "NEGATIVE_KWH_READING"
    voltage = rec.get("voltage")
    if voltage is None or not (180 <= voltage <= 260):
        return False, "VOLTAGE_OUT_OF_RANGE"
    pf = rec.get("power_factor")
    if pf is None or not (0 <= pf <= 1):
        return False, "POWER_FACTOR_OUT_OF_RANGE"
    if not rec.get("meter_id") or not rec.get("ward_id"):
        return False, "MISSING_ID_FIELD"
    return True, None


def wrap_dlq_record(source_topic: str, original_record: dict, error_reason: str) -> dict:
    """Wraps a failed record for the urbanpulse.dlq topic."""
    return {
        "source_topic": source_topic,
        "original_record": original_record,
        "error_reason": error_reason,
    }
