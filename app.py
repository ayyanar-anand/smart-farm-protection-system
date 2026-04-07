"""
Smart Farm Protection System — Flask Backend
=============================================
Serves the dashboard UI and provides REST API endpoints.
The ESP32 publishes sensor data to HiveMQ public MQTT broker;
this app subscribes and stores everything in-memory for the
frontend to query via the same API endpoints as before.

MQTT topic: smartfarm/<device_id>/data
"""

import json
import threading
from datetime import datetime, timedelta, timezone

from flask import Flask, render_template, request, jsonify

import paho.mqtt.client as mqtt

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT   = 1883
MQTT_TOPIC  = "smartfarm/+/data"          # subscribe to all devices

DEFAULT_DEVICE = "farm_unit_01"

# ---------------------------------------------------------------------------
# In-Memory Data Stores
# ---------------------------------------------------------------------------

_live_data = {}       # latest reading per device  { device_id: {…} }
_history   = []       # every reading (list of dicts)
_events    = []       # detection events (system_active rising edge)

# Track previous system_active per device for rising-edge detection
_prev_system_active = {}

# Thread lock for concurrent access
_lock = threading.Lock()

# ---------------------------------------------------------------------------
# MQTT Callbacks
# ---------------------------------------------------------------------------

def _on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"✅ MQTT connected to {MQTT_BROKER}")
        client.subscribe(MQTT_TOPIC)
        print(f"📡 Subscribed to: {MQTT_TOPIC}")
    else:
        print(f"⚠  MQTT connection failed, rc={rc}")


def _on_message(client, userdata, msg):
    """Handle incoming sensor data from ESP32 via MQTT."""
    try:
        payload = json.loads(msg.payload.decode())
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f"⚠  Bad MQTT payload: {e}")
        return

    device_id = payload.get("device_id", DEFAULT_DEVICE)

    with _lock:
        # 1. Update live data
        _live_data[device_id] = payload

        # 2. Append to history
        _history.append(payload)

        # 3. Rising-edge detection for events
        system_active = payload.get("system_active", False)
        prev_active = _prev_system_active.get(device_id, False)

        if system_active and not prev_active:
            _events.append(payload)

        _prev_system_active[device_id] = system_active


# ---------------------------------------------------------------------------
# MQTT Client (runs in background thread)
# ---------------------------------------------------------------------------

mqtt_client = mqtt.Client(
    client_id="flask_farm_dashboard",
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
)
mqtt_client.on_connect = _on_connect
mqtt_client.on_message = _on_message

def _start_mqtt():
    """Connect and start the MQTT network loop in a daemon thread."""
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        mqtt_client.loop_start()       # non-blocking background thread
    except Exception as e:
        print(f"⚠  MQTT start failed: {e}")

# ---------------------------------------------------------------------------
# Flask App
# ---------------------------------------------------------------------------

app = Flask(__name__)


# ===========================  HELPER FUNCTIONS  ============================

def _ts_to_str(ts):
    """Convert a Unix timestamp (int/float) to a human-readable string."""
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts)


# ===========================  PAGE ROUTES  =================================

@app.route("/")
def index():
    """Live Dashboard — shows real-time sensor cards, chart, and recent events."""
    return render_template("index.html")


@app.route("/history")
def history():
    """Detection event log with date-range filtering."""
    return render_template("history.html")


@app.route("/sessions")
def sessions():
    """Detection session summaries and daily bar chart."""
    return render_template("sessions.html")


@app.route("/stats")
def stats():
    """Sensor statistics and analytics page."""
    return render_template("stats.html")


# ===========================  API ROUTES  ==================================

# ── GET /api/live — latest sensor reading ──────────────────────────────────
@app.route("/api/live")
def api_live():
    """Return the most recent sensor reading from in-memory store."""
    with _lock:
        data = _live_data.get(DEFAULT_DEVICE, {})
    return jsonify(data)


# ── GET /api/history — all history, optional date range ────────────────────
@app.route("/api/history")
def api_history():
    """
    Return all history records. Supports optional query params:
      ?start=YYYY-MM-DD&end=YYYY-MM-DD
    to filter by timestamp range.
    """
    with _lock:
        records = list(_history)  # shallow copy

    if not records:
        return jsonify([])

    # Optional date-range filtering
    start_str = request.args.get("start")
    end_str = request.args.get("end")
    if start_str:
        try:
            start_ts = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
            records = [r for r in records if r.get("timestamp", 0) >= start_ts]
        except ValueError:
            pass
    if end_str:
        try:
            # Include the entire end day
            end_ts = (datetime.strptime(end_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                      + timedelta(days=1)).timestamp()
            records = [r for r in records if r.get("timestamp", 0) < end_ts]
        except ValueError:
            pass

    # Sort newest first
    records.sort(key=lambda r: r.get("timestamp", 0), reverse=True)
    return jsonify(records)


# ── GET /api/events — animal detection events only ─────────────────────────
@app.route("/api/events")
def api_events():
    """Return all animal detection events (system_active flipped true)."""
    with _lock:
        records = list(_events)
    records.sort(key=lambda r: r.get("timestamp", 0), reverse=True)
    return jsonify(records)


# ── GET /api/sessions — grouped detection sessions ────────────────────────
@app.route("/api/sessions")
def api_sessions():
    """
    Group events into "sessions" — continuous periods where system_active was
    true. Returns a list of session objects.
    """
    with _lock:
        records = list(_history)

    if not records:
        return jsonify([])

    records.sort(key=lambda r: r.get("timestamp", 0))

    # Walk through records and group consecutive system_active=true readings
    sessions_list = []
    current_session = None
    session_id = 0

    for rec in records:
        active = rec.get("system_active", False)
        ts = rec.get("timestamp", 0)

        if active:
            if current_session is None:
                session_id += 1
                current_session = {
                    "session_id": session_id,
                    "start": ts,
                    "end": ts,
                    "peak_temp": rec.get("temperature_c", 0),
                    "peak_sound": rec.get("sound_value", 0),
                    "peak_distance": rec.get("distance_cm", 0),
                    "event_count": 1,
                }
            else:
                current_session["end"] = ts
                current_session["event_count"] += 1
                current_session["peak_temp"] = max(
                    current_session["peak_temp"], rec.get("temperature_c", 0)
                )
                current_session["peak_sound"] = max(
                    current_session["peak_sound"], rec.get("sound_value", 0)
                )
                current_session["peak_distance"] = max(
                    current_session["peak_distance"], rec.get("distance_cm", 0)
                )
        else:
            if current_session is not None:
                duration = current_session["end"] - current_session["start"]
                current_session["duration_s"] = round(duration, 1)
                sessions_list.append(current_session)
                current_session = None

    # Close any still-open session
    if current_session is not None:
        duration = current_session["end"] - current_session["start"]
        current_session["duration_s"] = round(duration, 1)
        sessions_list.append(current_session)

    sessions_list.reverse()  # newest first
    return jsonify(sessions_list)


# ── GET /api/stats — aggregated statistics ─────────────────────────────────
@app.route("/api/stats")
def api_stats():
    """
    Return aggregated statistics:
      - detection counts (today / this week / all time)
      - average sensor values over last 24 h
      - per-sensor trigger frequency
      - detections per hour today
    """
    with _lock:
        records = list(_history)

    if not records:
        return jsonify({
            "today": 0, "week": 0, "all_time": 0,
            "avg_temp": 0, "avg_sound": 0, "avg_vib": 0, "avg_distance": 0,
            "sensor_triggers": {"pir": 0, "sound": 0, "vibration": 0, "distance": 0, "thermal": 0},
            "hourly": [0] * 24,
        })

    now = datetime.now(tz=timezone.utc)
    start_of_today = now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    start_of_week = (now - timedelta(days=now.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    ).timestamp()
    last_24h = (now - timedelta(hours=24)).timestamp()

    # Counters
    today_count = 0
    week_count = 0
    all_time_count = 0

    # Averages (last 24 h)
    sum_temp, sum_sound, sum_vib, sum_dist, count_24h = 0, 0, 0, 0, 0

    # Per-sensor trigger frequency (all time)
    sensor_triggers = {"pir": 0, "sound": 0, "vibration": 0, "distance": 0, "thermal": 0}

    # Hourly detections today
    hourly = [0] * 24

    for rec in records:
        ts = rec.get("timestamp", 0)
        active = rec.get("system_active", False)

        if active:
            all_time_count += 1
            if ts >= start_of_week:
                week_count += 1
            if ts >= start_of_today:
                today_count += 1
                # Hourly bucket
                try:
                    hour = datetime.fromtimestamp(ts, tz=timezone.utc).hour
                    hourly[hour] += 1
                except Exception:
                    pass

        # Sensor trigger counts
        if rec.get("pir"):
            sensor_triggers["pir"] += 1
        if rec.get("sound_detected"):
            sensor_triggers["sound"] += 1
        if rec.get("vib_detected"):
            sensor_triggers["vibration"] += 1
        if rec.get("distance_detected"):
            sensor_triggers["distance"] += 1
        if rec.get("thermal_detected"):
            sensor_triggers["thermal"] += 1

        # 24-hour averages
        if ts >= last_24h:
            sum_temp += rec.get("temperature_c", 0)
            sum_sound += rec.get("sound_value", 0)
            sum_vib += rec.get("vib_value", 0)
            sum_dist += rec.get("distance_cm", 0)
            count_24h += 1

    return jsonify({
        "today": today_count,
        "week": week_count,
        "all_time": all_time_count,
        "avg_temp": round(sum_temp / count_24h, 1) if count_24h else 0,
        "avg_sound": round(sum_sound / count_24h, 1) if count_24h else 0,
        "avg_vib": round(sum_vib / count_24h, 1) if count_24h else 0,
        "avg_distance": round(sum_dist / count_24h, 1) if count_24h else 0,
        "sensor_triggers": sensor_triggers,
        "hourly": hourly,
    })


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    _start_mqtt()
    app.run(debug=True)
