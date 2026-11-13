"""CST8916: demonstrate a clickstream's journey through Azure Event Hubs.

The /track route produces events. A background consumer receives them and keeps
the latest 50 in memory. The dashboard polls /api/events to display that buffer.
"""

import atexit
import json
import os
import threading
from datetime import datetime, timezone

from azure.core.exceptions import AzureError
from azure.eventhub import EventData, EventHubConsumerClient, EventHubProducerClient
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Configuration: use terminal variables locally or App Service app settings.
CONNECTION_STR = os.environ.get("EVENT_HUB_CONNECTION_STR", "").strip()
EVENT_HUB_NAME = os.environ.get("EVENT_HUB_NAME", "clickstream").strip()
MAX_BUFFER = 50
VALID_EVENT_TYPES = {
    "page_view", "product_click", "add_to_cart", "purchase", "banner_click"
}

if not CONNECTION_STR or not EVENT_HUB_NAME:
    raise RuntimeError(
        "Azure Event Hubs is required. Set EVENT_HUB_CONNECTION_STR and "
        "EVENT_HUB_NAME before starting the app."
    )

# Shared state: Flask handles requests while the consumer runs on another thread.
# The lock protects the buffer and its status while either thread accesses them.
_event_buffer = []
_buffer_lock = threading.Lock()

_consumer = None
_consumer_thread = None
_consumer_lock = threading.Lock()
_consumer_stopping = threading.Event()
_consumer_status = {
    "state": "stopped",
    "message": "Event Hubs consumer has not started.",
    "last_received_at": None,
}


def get_consumer_status():
    """Return a copy so callers cannot change the shared status."""
    with _buffer_lock:
        return dict(_consumer_status)


def _set_consumer_status(state, message):
    with _buffer_lock:
        _consumer_status.update(state=state, message=message)


# Producer: publish one browser action as a JSON event.
def send_to_event_hubs(event_dict: dict) -> None:
    """Serialize event_dict to JSON and publish it to Event Hubs."""
    # A client per request keeps connection cleanup visible for this small lab.
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        eventhub_name=EVENT_HUB_NAME,
    )
    with producer:
        # Even one event goes in a batch; add() checks the allowed batch size.
        event_batch = producer.create_batch()
        event_batch.add(EventData(json.dumps(event_dict)))
        producer.send_batch(event_batch)


# Consumer: receive from Azure before adding anything to the dashboard buffer.
def _on_event(partition_context, event):
    """The SDK calls this function for each event it receives."""
    if event is None:
        return

    try:
        data = json.loads(event.body_as_str(encoding="UTF-8"))
    except (json.JSONDecodeError, TypeError):
        app.logger.warning("Ignoring an Event Hubs event without valid UTF-8 JSON")
        return
    if not isinstance(data, dict):
        app.logger.warning("Ignoring an Event Hubs event that is not a JSON object")
        return

    with _buffer_lock:
        _event_buffer.append(data)
        # This is a recent-event window, so discard the oldest item when full.
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


def _receive_events(consumer):
    """Keep the blocking receive loop on the background thread."""
    try:
        with consumer:
            # "-1" reads from the earliest event still retained in each partition.
            consumer.receive(
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
                "error",
                "Event Hubs consumer stopped unexpectedly. "
                "Check settings and restart the app.",
            )


def start_consumer():
    """Start one consumer thread so Flask can keep serving HTTP requests."""
    global _consumer, _consumer_thread
    with _consumer_lock:
        if _consumer_thread is not None and _consumer_thread.is_alive():
            return

        try:
            _consumer = EventHubConsumerClient.from_connection_string(
                conn_str=CONNECTION_STR,
                consumer_group="$Default",  # Built-in group; available on Basic.
                eventhub_name=EVENT_HUB_NAME,
            )
        except ValueError:
            raise RuntimeError(
                "Invalid Event Hubs configuration. Check EVENT_HUB_CONNECTION_STR "
                "and EVENT_HUB_NAME."
            ) from None

        _consumer_stopping.clear()
        # Starting a thread does not yet confirm that Azure is reachable.
        _set_consumer_status(
            "waiting", "Waiting to receive events from Azure Event Hubs."
        )
        _consumer_thread = threading.Thread(
            target=_receive_events,
            args=(_consumer,),
            name="event-hubs-consumer",
            daemon=True,
        )
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


# HTTP routes: browser pages, event publishing, dashboard data, and status.
@app.route("/")
def index():
    """Serve the demo e-commerce store."""
    return send_from_directory("templates", "client.html")


@app.route("/dashboard")
def dashboard():
    """Serve the live analytics dashboard."""
    return send_from_directory("templates", "dashboard.html")


@app.route("/track", methods=["POST"])
def track():
    """Validate a browser event, add a UTC timestamp, and publish it to Azure."""
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict) or not payload:
        return jsonify({
            "error": "Expected a JSON object containing a click event."
        }), 400

    event_type = payload.get("event_type")
    if not isinstance(event_type, str) or event_type not in VALID_EVENT_TYPES:
        return jsonify({"error": "Unsupported or missing event_type."}), 400
    for field in ("page", "product_id", "user_id", "session_id"):
        value = payload.get(field)
        if value is not None and not isinstance(value, str):
            return jsonify({"error": f"{field} must be a string or null."}), 400

    event = {
        "event_type": event_type,
        "page": payload.get("page", "/"),
        "product_id": payload.get("product_id"),
        "user_id": payload.get("user_id", "anonymous"),
        "session_id": payload.get("session_id"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    try:
        send_to_event_hubs(event)
    except (AzureError, ValueError) as error:
        app.logger.error("Event Hubs publish failed (%s)", type(error).__name__)
        return jsonify({
            "error": (
                "Could not confirm publishing to Azure Event Hubs. Check the "
                "connection string, hub name, Send permission, and network access."
            )
        }), 503

    # Only _on_event may write to the dashboard buffer. Publishing successfully
    # does not mean the consumer has received the event yet.
    return jsonify({"status": "published", "event": event}), 201


@app.route("/api/events", methods=["GET"])
def get_events():
    """Return recent events and counts for the dashboard's two-second polling.

    Optional query parameter: ?limit=20 (default 20, allowed range 1–50).
    """
    try:
        # Convert the URL value to an integer and keep it within the buffer size.
        limit = max(1, min(int(request.args.get("limit", 20)), MAX_BUFFER))
    except ValueError:
        limit = 20

    with _buffer_lock:
        recent = list(_event_buffer[-limit:])

    # Counts describe this response's window, not all events ever published.
    summary = {}
    for event in recent:
        event_type = event.get("event_type", "unknown")
        if not isinstance(event_type, str):
            event_type = "unknown"
        summary[event_type] = summary.get(event_type, 0) + 1

    return jsonify({
        "events": recent,
        "summary": summary,
        "total": len(recent),
        "consumer": get_consumer_status(),
    }), 200


@app.route("/health", methods=["GET"])
def health():
    """Report known consumer failures; waiting alone does not verify connectivity."""
    status = get_consumer_status()
    failed = status["state"] in {"error", "stopped"}
    status_code = 503 if failed else 200
    return jsonify({
        "status": "degraded" if failed else "running",
        "consumer": status,
    }), status_code


# Start on import too: App Service's Gunicorn server loads this module as app:app.
# Keep one worker and one instance because each process has its own event buffer.
start_consumer()
atexit.register(stop_consumer)


if __name__ == "__main__":
    # Disable the reloader so it does not start a second consumer.
    app.run(debug=False, host="0.0.0.0", port=8000)
