# -*- coding: utf-8 -*-
"""
app.py
======
DeliveryReportPro — Flask + Flask-SocketIO local server.
Serves the UI on localhost:5173, manages pipeline execution,
handles prompt responses from the UI, and streams progress via WebSockets.
"""

import os
import sys
import threading
import webbrowser
from datetime import datetime
from pathlib import Path

from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit

from config import (
    is_configured, load_config, save_config,
    validate_config, wipe_config, REQUIRED_KEYS
)
from pipeline import run_pipeline, PipelineAbort

# ── App setup ──────────────────────────────────────────────────────

app = Flask(__name__)
app.config["SECRET_KEY"] = os.urandom(24)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

PORT = 5173

# ── Pipeline state (per session, single-user desktop app) ──────────

_pipeline_state = {
    "running":        False,
    "cancelled":      False,
    "prompt_pending": None,   # prompt_id waiting for answer
    "prompt_event":   None,   # threading.Event to unblock pipeline
    "prompt_answer":  None,   # answer from UI
    "delivery_date":  None,
    "answers":        {},     # accumulated prompt answers
}


# ── Helpers ────────────────────────────────────────────────────────

def _emit(event: str, data: dict) -> None:
    """Thread-safe SocketIO emit."""
    socketio.emit(event, data)


def _prompt_handler(prompt_id: str, context: dict):
    """
    Called by the pipeline when it needs a user answer.
    Blocks the pipeline thread until the UI responds.
    """
    evt = threading.Event()
    _pipeline_state["prompt_pending"] = prompt_id
    _pipeline_state["prompt_event"]   = evt
    _pipeline_state["prompt_answer"]  = None
    _emit("prompt", {"id": prompt_id, "context": context})
    evt.wait(timeout=300)  # 5-minute timeout
    answer = _pipeline_state["prompt_answer"]
    _pipeline_state["prompt_pending"] = None
    _pipeline_state["prompt_event"]   = None
    return answer


# ── HTTP routes ────────────────────────────────────────────────────

@app.route("/")
def index():
    configured = is_configured()
    return render_template("index.html", configured=configured)


@app.route("/api/status")
def status():
    return jsonify({
        "configured": is_configured(),
        "running":    _pipeline_state["running"],
    })


@app.route("/api/setup", methods=["POST"])
def setup():
    """Save credentials on first run."""
    data = request.get_json()
    errors = validate_config(data)
    if errors:
        return jsonify({"ok": False, "missing": errors}), 400
    try:
        save_config(data)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/config", methods=["GET"])
def get_config_keys():
    """Returns which keys are present (not values) for the settings UI."""
    if not is_configured():
        return jsonify({"configured": False})
    try:
        cfg = load_config()
        return jsonify({
            "configured": True,
            "keys": {k: bool(cfg.get(k)) for k in REQUIRED_KEYS}
        })
    except Exception as e:
        return jsonify({"configured": False, "error": str(e)})


@app.route("/api/config", methods=["POST"])
def update_config():
    """Update one or more config values."""
    data = request.get_json()
    try:
        existing = load_config() if is_configured() else {}
        existing.update({k: v for k, v in data.items() if v})
        save_config(existing)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/dates")
def get_dates():
    """Returns next 7 business days for the date picker."""
    from datetime import timedelta
    today = datetime.today()
    dates = []
    d = today
    while len(dates) < 7:
        d += timedelta(days=1)
        if d.weekday() < 5:
            dates.append({
                "value": d.strftime("%Y-%m-%d"),
                "label": d.strftime("%A, %B %d %Y"),
                "short": d.strftime("%b %d"),
            })
    return jsonify(dates)


@app.route("/api/cancel", methods=["POST"])
def cancel_pipeline():
    """Signals the running pipeline to stop at the next checkpoint."""
    if _pipeline_state["running"]:
        _pipeline_state["cancelled"] = True
        # Unblock any pending prompt so the thread can exit
        if _pipeline_state["prompt_event"]:
            _pipeline_state["prompt_event"].set()
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": "Not running"})


@app.route("/api/update-credentials", methods=["POST"])
def update_credentials():
    """
    Verifies current HS email+password then applies updates.
    Requires correct current credentials to change anything.
    """
    data            = request.get_json()
    current_email   = data.get("current_email", "").strip()
    current_password= data.get("current_password", "").strip()
    updates         = data.get("updates", {})

    if not current_email or not current_password:
        return jsonify({"ok": False, "error": "Current email and password are required"}), 400

    try:
        from config import update_credentials as _update
        _update(current_email, current_password, updates)
        return jsonify({"ok": True})
    except PermissionError as e:
        return jsonify({"ok": False, "error": str(e)}), 403
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/open-report", methods=["POST"])
def open_report():
    """Opens the generated Excel file in the default application."""
    data = request.get_json()
    path = data.get("path", "")
    if not path or not Path(path).exists():
        return jsonify({"ok": False, "error": "File not found"}), 404
    try:
        os.startfile(path)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ── SocketIO events ────────────────────────────────────────────────

@socketio.on("connect")
def on_connect():
    emit("connected", {
        "configured": is_configured(),
        "running":    _pipeline_state["running"],
    })


@socketio.on("start_pipeline")
def on_start_pipeline(data):
    """
    Triggered by UI run button.
    data: { "date": "YYYY-MM-DD" }
    """
    if _pipeline_state["running"]:
        emit("error", {"message": "Pipeline already running"})
        return

    if not is_configured():
        emit("error", {"message": "Not configured — complete setup first"})
        return

    date_str     = data.get("date")
    diesel_price = float(data.get("diesel_price", 3.80))
    use_cache    = bool(data.get("use_cache", False))
    try:
        delivery_date = datetime.strptime(date_str, "%Y-%m-%d")
    except (ValueError, TypeError):
        emit("error", {"message": f"Invalid date: {date_str}"})
        return

    try:
        config = load_config()
    except Exception as e:
        emit("error", {"message": f"Config error: {e}"})
        return

    _pipeline_state["running"]       = True
    _pipeline_state["cancelled"]     = False
    _pipeline_state["delivery_date"] = delivery_date
    _pipeline_state["diesel_price"]  = diesel_price
    _pipeline_state["answers"]       = {}

    def run():
        try:
            cfg = dict(config)
            cfg["USE_CACHE"] = use_cache
            result = run_pipeline(
                config        = cfg,
                delivery_date = delivery_date,
                diesel_price  = _pipeline_state.get("diesel_price", 3.80),
                emit_fn       = _emit,
                prompt_answers= _pipeline_state["answers"],
            )
            _emit("pipeline_complete", {
                "filepath":   result.get("filepath"),
                "fin_rows":   result.get("fin_rows", []),
                "errors":     result.get("errors", []),
                "warnings":   result.get("warnings", []),
                "report_text": result.get("report_text", ""),
            })
        except Exception as e:
            _emit("pipeline_error", {"message": str(e)})
        finally:
            _pipeline_state["running"] = False

    thread = threading.Thread(target=run, daemon=True)
    thread.start()


@socketio.on("prompt_answer")
def on_prompt_answer(data):
    """
    Receives answer from UI prompt card.
    data: { "id": prompt_id, "value": answer }
    """
    prompt_id = data.get("id")
    value     = data.get("value")

    if not prompt_id:
        return

    # Always store — prompt_int polls _pipeline_state["answers"]
    _pipeline_state["answers"][prompt_id] = value
    _pipeline_state["prompt_answer"]      = value
    _pipeline_state["prompt_pending"]     = None

    # Log receipt so we can confirm in the feed
    _emit("log", {"message": f"  Server received: {prompt_id} = {value}", "level": "info"})

    # Unblock any threading.Event waiting
    if _pipeline_state["prompt_event"]:
        _pipeline_state["prompt_event"].set()


@socketio.on("disconnect")
def on_disconnect():
    pass


# ── Entry point ────────────────────────────────────────────────────

def open_browser():
    """Opens the browser after a short delay to let Flask start."""
    import time
    time.sleep(1.2)
    webbrowser.open(f"http://localhost:{PORT}")


if __name__ == "__main__":
    threading.Thread(target=open_browser, daemon=True).start()
    socketio.run(app, host="127.0.0.1", port=PORT, debug=False,
                 use_reloader=False, allow_unsafe_werkzeug=True)
