# CST8916 – Week 10 Lab: Clickstream Analytics with Azure Event Hubs
#
# This Flask app has two roles:
#   1. PRODUCER  – receives click events from the browser and sends them to Azure Event Hubs
#   2. CONSUMER  – reads Event Hubs and buffers the last 50 events for a dashboard
#
# Routes:
#   GET  /              → serves the demo e-commerce store (client.html)
#   POST /track         → receives a click event and publishes it to Event Hubs
#   GET  /dashboard     → serves the live analytics dashboard (dashboard.html)
#   GET  /api/events    → returns recent events as JSON (polled by the dashboard)

import atexit
import os
import json
import threading
from datetime import datetime, timezone

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

# ---------------------------------------------------------------------------
# Azure Event Hubs SDK
# EventHubProducerClient  – sends events to Event Hubs
# EventHubConsumerClient  – reads events from Event Hubs
# EventData               – wraps a single event payload
# ---------------------------------------------------------------------------
from azure.eventhub import EventHubProducerClient, EventHubConsumerClient, EventData
from azure.core.exceptions import AzureError

app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)

# ---------------------------------------------------------------------------
# Configuration – read from environment variables so secrets never live in code
#
# Set these before running locally:
#   export EVENT_HUB_CONNECTION_STR="Endpoint=sb://..."
#   export EVENT_HUB_NAME="clickstream"
#
# On Azure App Service, set them as Application Settings in the portal.
# ---------------------------------------------------------------------------
CONNECTION_STR = os.environ.get("EVENT_HUB_CONNECTION_STR", "").strip()
EVENT_HUB_NAME = os.environ.get("EVENT_HUB_NAME", "clickstream").strip()

if not CONNECTION_STR or not EVENT_HUB_NAME:
    raise RuntimeError(
        "Azure Event Hubs is required. Set EVENT_HUB_CONNECTION_STR and "
        "EVENT_HUB_NAME before starting the app."
    )

# In-memory buffer: stores the last 50 events received by the consumer thread.
# In a production system you would query a database or Azure Stream Analytics output.
_event_buffer = []
_buffer_lock = threading.Lock()
MAX_BUFFER = 50
_consumer = None
_consumer_thread = None
_consumer_lock = threading.Lock()
_consumer_stopping = threading.Event()
_consumer_status = {
    "state": "stopped",
    "message": "Event Hubs consumer has not started.",
    "last_received_at": None,
}


def consumer_status():
    """Return a snapshot without treating a started thread as a verified connection."""
    with _buffer_lock:
        return dict(_consumer_status)


def _set_consumer_status(state, message):
    with _buffer_lock:
        _consumer_status.update(state=state, message=message)


# ---------------------------------------------------------------------------
# Helper – send a single event dict to Azure Event Hubs
# ---------------------------------------------------------------------------
def send_to_event_hubs(event_dict: dict):
    """Serialize event_dict to JSON and publish it to Event Hubs."""
    # EventHubProducerClient is created fresh per request here for simplicity.
    # In a high-throughput production app you would keep a shared client instance.
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        eventhub_name=EVENT_HUB_NAME,
    )
    with producer:
        # create_batch() lets the SDK manage event size limits automatically
        event_batch = producer.create_batch()
        event_batch.add(EventData(json.dumps(event_dict)))
        producer.send_batch(event_batch)


# ---------------------------------------------------------------------------
# Background consumer thread – reads events from Event Hubs and buffers them
# ---------------------------------------------------------------------------
def _on_event(partition_context, event):
    """Callback invoked by the consumer client for each incoming event."""
    if event is None:
        return

    body = event.body_as_str(encoding="UTF-8")
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        app.logger.warning("Ignoring an Event Hubs event with invalid JSON")
        return
    if not isinstance(data, dict):
        app.logger.warning("Ignoring an Event Hubs event that is not a JSON object")
        return

    with _buffer_lock:
        _event_buffer.append(data)
        # Keep the buffer at MAX_BUFFER entries (drop the oldest)
        if len(_event_buffer) > MAX_BUFFER:
            _event_buffer.pop(0)
        _consumer_status.update(
            state="receiving",
            message="Events received from Azure Event Hubs.",
            last_received_at=datetime.now(timezone.utc).isoformat(),
        )

    # Remember the processed position in this client's memory. This lab does not
    # use a persistent checkpoint store; a restart replays retained events.
    partition_context.update_checkpoint(event)


def _on_consumer_error(partition_context, error):
    # Keep credentials and SDK exception details out of HTTP responses and logs.
    app.logger.error("Event Hubs receive failed (%s)", type(error).__name__)
    _set_consumer_status(
        "error",
        "Unable to receive events from Azure Event Hubs. Check the connection "
        "string, hub name, Listen permission, and network access.",
    )


def start_consumer():
    """Start the Event Hubs consumer in a background daemon thread.

    The consumer must run on a separate thread because consumer.receive() blocks
    forever waiting for events. Running it on the main thread would freeze Flask
    and make the web server unable to handle any HTTP requests.
    """
    global _consumer, _consumer_thread
    with _consumer_lock:
        if _consumer_thread is not None and _consumer_thread.is_alive():
            return

        # $Default is the built-in consumer group. No checkpoint store is used
        # in this small lab, so all retained events are replayed on startup.
        try:
            _consumer = EventHubConsumerClient.from_connection_string(
                conn_str=CONNECTION_STR,
                consumer_group="$Default",
                eventhub_name=EVENT_HUB_NAME,
            )
        except ValueError:
            raise RuntimeError(
                "Invalid Event Hubs configuration. Check EVENT_HUB_CONNECTION_STR "
                "and EVENT_HUB_NAME."
            ) from None

        _consumer_stopping.clear()
        _set_consumer_status("waiting", "Waiting to receive events from Azure Event Hubs.")

        def run():
            try:
                with _consumer:
                    # receive() blocks and invokes _on_event for each delivery.
                    _consumer.receive(
                        on_event=_on_event,
                        on_error=_on_consumer_error,
                        starting_position="-1",
                    )
            except Exception as error:
                if not _consumer_stopping.is_set():
                    _on_consumer_error(None, error)
            finally:
                if _consumer_stopping.is_set():
                    _set_consumer_status("stopped", "Event Hubs consumer stopped.")
                else:
                    _set_consumer_status(
                        "error", "Event Hubs consumer stopped unexpectedly. Check settings and restart the app."
                    )

        _consumer_thread = threading.Thread(target=run, name="event-hubs-consumer", daemon=True)
        _consumer_thread.start()
        app.logger.info("Event Hubs consumer thread started; waiting for events")


def stop_consumer():
    """Close the receive connection when the local server or Gunicorn worker exits."""
    with _consumer_lock:
        _consumer_stopping.set()
        if _consumer is not None:
            _consumer.close()
        if _consumer_thread is not None:
            _consumer_thread.join(timeout=5)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    """Serve the demo e-commerce store."""
    return send_from_directory("templates", "client.html")


@app.route("/dashboard")
def dashboard():
    """Serve the live analytics dashboard."""
    return send_from_directory("templates", "dashboard.html")


@app.route("/health", methods=["GET"])
def health():
    """Report consumer failures separately from merely running the web server."""
    status = consumer_status()
    failed = status["state"] in {"error", "stopped"}
    return jsonify({"status": "degraded" if failed else "running", "consumer": status}), 503 if failed else 200


@app.route("/track", methods=["POST"])
def track():
    """
    Receive a click event from the browser and publish it to Event Hubs.

    Expected JSON body:
    {
        "event_type": "page_view" | "product_click" | "add_to_cart" | "purchase",
        "page":       "/products/shoes",
        "product_id": "p_shoe_42",       (optional)
        "user_id":    "u_1234"
    }
    """
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict) or not payload:
        return jsonify({"error": "Expected a JSON object containing a click event."}), 400

    event_type = payload.get("event_type")
    if not isinstance(event_type, str) or event_type not in {
        "page_view", "product_click", "add_to_cart", "purchase", "banner_click"
    }:
        return jsonify({"error": "Unsupported or missing event_type."}), 400
    for field in ("page", "product_id", "user_id", "session_id"):
        if field in payload and not isinstance(payload[field], str) and payload[field] is not None:
            return jsonify({"error": f"{field} must be a string or null."}), 400

    # Enrich the event with a server-side timestamp
    event = {
        "event_type": event_type,
        "page":       payload.get("page", "/"),
        "product_id": payload.get("product_id"),
        "user_id":    payload.get("user_id", "anonymous"),
        "session_id": payload.get("session_id"),
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }

    try:
        send_to_event_hubs(event)
    except (AzureError, ValueError) as error:
        app.logger.error("Event Hubs publish failed (%s)", type(error).__name__)
        return jsonify({
            "error": "Could not confirm publishing to Azure Event Hubs. Check the "
                     "connection string, hub name, Send permission, and network access."
        }), 503

    # Only _on_event may write to the dashboard buffer. Publishing successfully
    # does not mean the consumer has received the event yet.
    return jsonify({"status": "published", "event": event}), 201


@app.route("/api/events", methods=["GET"])
def get_events():
    """
    Return the buffered events as JSON.
    The dashboard polls this endpoint every 2 seconds.

    Optional query param:  ?limit=20  (default 20, max 50)
    """
    try:
        # request.args.get("limit", 20) reads ?limit=N from the URL, defaulting to 20.
        # int() converts it from a string (all URL params are strings) to an integer.
        # Clamp the requested window to 1–50 events.
        limit = max(1, min(int(request.args.get("limit", 20)), MAX_BUFFER))
    except ValueError:
        # int() raises ValueError if the param is non-numeric (e.g. ?limit=abc)
        limit = 20

    with _buffer_lock:
        recent = list(_event_buffer[-limit:])

    # Build a simple summary for the dashboard
    summary = {}
    for e in recent:
        et = e.get("event_type", "unknown")
        if not isinstance(et, str):
            et = "unknown"
        summary[et] = summary.get(et, 0) + 1

    return jsonify({
        "events": recent, "summary": summary, "total": len(recent),
        "consumer": consumer_status(),
    }), 200


# ---------------------------------------------------------------------------
# Initialize on import as well as direct execution. Azure App Service detects
# app.py automatically and imports app:app with Gunicorn. gunicorn.conf.py keeps
# one worker and disables preloading so the thread starts in the serving process.
# ---------------------------------------------------------------------------
start_consumer()
atexit.register(stop_consumer)


if __name__ == "__main__":
    # Run on 0.0.0.0 so it is reachable both locally and inside Azure App Service
    app.run(debug=False, host="0.0.0.0", port=8000)
