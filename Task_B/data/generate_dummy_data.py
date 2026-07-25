"""
Generates dummy input files for UrbanPulse Task B, matching the schemas
specified in the assignment brief so producer/consumer/streams code is
directly runnable.

Output (written into the SAME folder this script lives in, i.e. Task_B/data,
regardless of where you run it from):
  route_schedule.csv       ~50 routes
  zone_profile.csv         ~18 zones
  bus_gps_sample.jsonl      ~8000 events
  traffic_signals_sample.jsonl  ~2500 events
  air_quality_sample.jsonl  ~1500 events (5% null AQI, some hazardous >300)
  smart_meters_sample.jsonl ~4000 events

DLQ VIOLATION COVERAGE (for Problem on DLQ / error handling):
  bus_gps        -> INVALID_LAT_LON, SPEED_OUT_OF_RANGE, OCCUPANCY_OUT_OF_RANGE
  air_quality    -> NULL_AQI
  smart_meters   -> VOLTAGE_OUT_OF_RANGE, NEGATIVE_KWH_READING
This covers 3 different topics with 5 distinct violation types total.
"""
import csv
import json
import os
import random
from datetime import datetime, timedelta

random.seed(42)
# Resolve relative to THIS script's location, not the current working
# directory -- so it always writes into Task_B/data regardless of whether
# you run it as `python generate_dummy_data.py` from inside data/, or as
# `python data\generate_dummy_data.py` from Task_B.
OUT_DIR = os.path.dirname(os.path.abspath(__file__))

ZONES = [f"Z{str(i).zfill(2)}" for i in range(1, 19)]
ZONE_NAMES = [
    "Andheri East", "Andheri West", "Bandra", "Borivali", "Chembur", "Colaba",
    "Dadar", "Dahisar", "Ghatkopar", "Goregaon", "Juhu", "Kandivali", "Kurla",
    "Malad", "Mulund", "Powai", "Thane", "Worli",
]
ROUTE_TERMINALS = ["Central Terminal", "North Depot", "East Depot", "South Hub", "West Hub"]

now = datetime(2026, 7, 1, 6, 0, 0)


def ts(offset_sec):
    return (now + timedelta(seconds=offset_sec)).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------- route_schedule.csv ----------
routes = []
with open(f"{OUT_DIR}/route_schedule.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["route_id", "route_name", "terminal", "scheduled_arrival_time"])
    for i in range(1, 51):
        route_id = f"R{str(i).zfill(3)}"
        route_name = f"{random.choice(ZONE_NAMES)} - {random.choice(ZONE_NAMES)} Express"
        terminal = random.choice(ROUTE_TERMINALS)
        sched = ts(random.randint(0, 3600) + i * 60)
        w.writerow([route_id, route_name, terminal, sched])
        routes.append(route_id)

# ---------- zone_profile.csv ----------
with open(f"{OUT_DIR}/zone_profile.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["zone", "zone_name", "population", "num_schools"])
    for z, name in zip(ZONES, ZONE_NAMES):
        w.writerow([z, name, random.randint(120000, 420000), random.randint(4, 25)])

# ---------- bus_gps_sample.jsonl (~8000 events, keyed conceptually by route_id) ----------
with open(f"{OUT_DIR}/bus_gps_sample.jsonl", "w") as f:
    n = 8000
    for i in range(n):
        route_id = random.choice(routes)
        bus_id = f"BUS{str(random.randint(1, 200)).zfill(4)}"
        rec = {
            "bus_id": bus_id,
            "route_id": route_id,
            "lat": round(19.0 + random.random() * 0.35, 6),
            "lon": round(72.75 + random.random() * 0.35, 6),
            "speed_kmh": round(random.uniform(0, 60), 1),
            "occupancy_pct": random.randint(0, 100),
            "timestamp": ts(i * 3),
        }
        # inject a handful of impossible coordinates for DLQ validation testing
        if random.random() < 0.004:
            rec["lat"] = round(random.uniform(-200, 200), 6)
        if random.random() < 0.003:
            rec["speed_kmh"] = round(random.uniform(150, 300), 1)  # impossible speed
        if random.random() < 0.003:
            rec["occupancy_pct"] = random.choice([-10, 150])  # impossible occupancy
        f.write(json.dumps(rec) + "\n")

# ---------- traffic_signals_sample.jsonl (~2500 events) ----------
with open(f"{OUT_DIR}/traffic_signals_sample.jsonl", "w") as f:
    n = 2500
    phases = ["GREEN", "RED", "AMBER"]
    for i in range(n):
        rec = {
            "junction_id": f"J{str(random.randint(1, 120)).zfill(3)}",
            "zone": random.choice(ZONES),
            "vehicle_count": random.randint(0, 300),
            "avg_wait_sec": round(random.uniform(5, 180), 1),
            "signal_phase": random.choice(phases),
            "timestamp": ts(i * 9),
        }
        f.write(json.dumps(rec) + "\n")

# ---------- air_quality_sample.jsonl (~1500 events, 5% null AQI, some hazardous) ----------
with open(f"{OUT_DIR}/air_quality_sample.jsonl", "w") as f:
    n = 1500
    for i in range(n):
        aqi = round(random.uniform(20, 180), 1)
        if random.random() < 0.02:
            aqi = round(random.uniform(301, 500), 1)  # hazardous outlier
        rec = {
            "sensor_id": f"S{str(random.randint(1, 90)).zfill(3)}",
            "zone": random.choice(ZONES),
            "pm25": round(random.uniform(5, 250), 1),
            "pm10": round(random.uniform(10, 350), 1),
            "no2": round(random.uniform(5, 120), 1),
            "aqi": aqi,
            "timestamp": ts(i * 15),
        }
        if random.random() < 0.05:
            rec["aqi"] = None
        f.write(json.dumps(rec) + "\n")

# ---------- smart_meters_sample.jsonl (~4000 events) ----------
with open(f"{OUT_DIR}/smart_meters_sample.jsonl", "w") as f:
    n = 4000
    for i in range(n):
        rec = {
            "meter_id": f"M{str(random.randint(1, 1000)).zfill(5)}",
            "ward_id": f"W{str(random.randint(1, 24)).zfill(2)}",
            "kwh_reading": round(random.uniform(0.5, 45.0), 2),
            "voltage": round(random.uniform(210, 250), 1),
            "power_factor": round(random.uniform(0.75, 1.0), 3),
            "timestamp": ts(i * 6),
        }
        if random.random() < 0.01:
            rec["voltage"] = round(random.uniform(0, 50), 1)  # brownout / bad reading
        if random.random() < 0.01:
            rec["kwh_reading"] = round(random.uniform(-10, -1), 2)  # negative reading
        f.write(json.dumps(rec) + "\n")

print("Dummy data generated in", OUT_DIR)