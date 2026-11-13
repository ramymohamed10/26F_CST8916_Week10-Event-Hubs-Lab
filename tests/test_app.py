"""Check the Event Hubs data path without Azure credentials or network calls."""

import atexit
import importlib.util
import json
import os
from pathlib import Path
import sys
import threading
import unittest
from unittest.mock import MagicMock, patch

from azure.core.exceptions import AzureError
from azure.eventhub import EventData


APP_PATH = Path(__file__).resolve().parents[1] / "app.py"
TEST_CONNECTION = (
    "Endpoint=sb://lab-test.servicebus.windows.net/;"
    "SharedAccessKeyName=test;SharedAccessKey=ZmFrZQ=="
)


class EventHubsLabTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch.dict(os.environ, {
            "EVENT_HUB_CONNECTION_STR": TEST_CONNECTION,
            "EVENT_HUB_NAME": "clickstream",
        }))
        self.release_receiver = threading.Event()
        self.receiver_started = threading.Event()
        self.addCleanup(self.release_receiver.set)

        self.consumer = MagicMock()
        self.consumer.__enter__.return_value = self.consumer
        self.consumer.close.side_effect = self.release_receiver.set

        def receive(**kwargs):
            self.receiver_started.set()
            self.release_receiver.wait()

        self.consumer.receive.side_effect = receive
        self.consumer_factory = self.enterContext(patch(
            "azure.eventhub.EventHubConsumerClient.from_connection_string",
            return_value=self.consumer,
        ))
        self.producer_factory = self.enterContext(patch(
            "azure.eventhub.EventHubProducerClient.from_connection_string"
        ))
        self.producer = self.producer_factory.return_value

        self.module = self.load_app("lab_under_test")
        self.addCleanup(atexit.unregister, self.module.stop_consumer)
        self.addCleanup(self.module.stop_consumer)
        self.assertTrue(self.receiver_started.wait(timeout=2), "Consumer did not start on import")
        self.module.app.testing = True
        self.client = self.module.app.test_client()

    def load_app(self, name):
        spec = importlib.util.spec_from_file_location(name, APP_PATH)
        module = importlib.util.module_from_spec(spec)
        self.enterContext(patch.dict(sys.modules, {name: module}))
        spec.loader.exec_module(module)
        return module

    def deliver(self, event):
        context = MagicMock()
        self.module._on_event(context, EventData(json.dumps(event)))
        return context

    def test_import_starts_consumer_and_start_is_idempotent(self):
        self.module.start_consumer()
        self.consumer_factory.assert_called_once_with(
            conn_str=TEST_CONNECTION, consumer_group="$Default", eventhub_name="clickstream"
        )
        self.consumer.receive.assert_called_once_with(
            on_event=self.module._on_event,
            on_error=self.module._on_consumer_error,
            starting_position="-1",
        )
        status = self.client.get("/api/events").json["consumer"]
        self.assertEqual(status["state"], "waiting")
        self.assertIsNone(status["last_received_at"])

    def test_publish_does_not_populate_dashboard_until_consumed(self):
        response = self.client.post("/track", json={
            "event_type": "add_to_cart", "page": "/products/shoes",
            "product_id": "p_shoe_01", "user_id": "round-trip-test", "session_id": "s_test",
        })
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json["status"], "published")
        self.assertEqual(self.client.get("/api/events").json["total"], 0)

        batch = self.producer.create_batch.return_value
        sent_event = batch.add.call_args.args[0]
        payload = json.loads(sent_event.body_as_str())
        self.assertEqual(payload, response.json["event"])
        self.assertEqual(payload["session_id"], "s_test")
        self.producer.send_batch.assert_called_once_with(batch)

        # Simulate the SDK delivering exactly the event that was published.
        context = self.deliver(payload)
        dashboard = self.client.get("/api/events").json
        self.assertEqual(dashboard["total"], 1)
        self.assertEqual(dashboard["events"], [payload])
        self.assertEqual(dashboard["summary"], {"add_to_cart": 1})
        self.assertEqual(dashboard["consumer"]["state"], "receiving")
        self.assertIsNotNone(dashboard["consumer"]["last_received_at"])
        context.update_checkpoint.assert_called_once()

    def test_publish_failures_return_json_error_without_buffering(self):
        for error in (AzureError("secret-sdk-detail"), ValueError("secret-sdk-detail")):
            with self.subTest(error=type(error).__name__):
                self.producer.send_batch.side_effect = error
                response = self.client.post("/track", json={"event_type": "page_view"})
                self.assertEqual(response.status_code, 503)
                self.assertIn("Could not confirm publishing", response.json["error"])
                self.assertNotIn("secret-sdk-detail", response.get_data(as_text=True))
                self.assertEqual(self.client.get("/api/events").json["total"], 0)

    def test_batch_creation_failure_is_not_reported_as_published(self):
        self.producer.create_batch.side_effect = AzureError("network failure")
        response = self.client.post("/track", json={"event_type": "page_view"})
        self.assertEqual(response.status_code, 503)
        self.producer.send_batch.assert_not_called()

    def test_invalid_payloads_are_rejected_before_publishing(self):
        for payload in ([], {}, "click", {"event_type": []}, {"event_type": "unsupported"},
                        {"event_type": "page_view", "user_id": {"bad": "value"}}):
            with self.subTest(payload=payload):
                response = self.client.post("/track", json=payload)
                self.assertEqual(response.status_code, 400)
                self.assertIn("error", response.json)
        response = self.client.post("/track", data="broken JSON", content_type="application/json")
        self.assertEqual(response.status_code, 400)
        self.producer_factory.assert_not_called()

    def test_consumer_errors_are_visible_and_do_not_hide_existing_events(self):
        self.deliver({"event_type": "page_view"})
        self.module._on_consumer_error(None, AzureError("secret-sdk-detail"))
        health = self.client.get("/health")
        self.assertEqual(health.status_code, 503)
        self.assertEqual(health.json["consumer"]["state"], "error")
        self.assertNotIn("secret-sdk-detail", health.get_data(as_text=True))
        dashboard = self.client.get("/api/events").json
        self.assertEqual(dashboard["total"], 1)
        self.assertEqual(dashboard["consumer"]["state"], "error")
        self.deliver({"event_type": "add_to_cart"})
        self.assertEqual(self.client.get("/health").status_code, 200)

    def test_consumer_returning_unexpectedly_is_reported_as_failure(self):
        self.release_receiver.set()
        self.module._consumer_thread.join(timeout=2)
        self.assertFalse(self.module._consumer_thread.is_alive())
        self.assertEqual(self.client.get("/health").status_code, 503)
        self.assertEqual(self.client.get("/api/events").json["consumer"]["state"], "error")

    def test_shutdown_closes_consumer_and_marks_it_stopped(self):
        self.module.stop_consumer()
        self.assertFalse(self.module._consumer_thread.is_alive())
        self.assertEqual(self.client.get("/health").json["consumer"]["state"], "stopped")

    def test_buffer_keeps_only_latest_fifty_received_events(self):
        for index in range(55):
            self.deliver({"event_type": "page_view", "user_id": str(index)})
        result = self.client.get("/api/events?limit=999").json
        self.assertEqual(result["total"], 50)
        self.assertEqual(result["events"][0]["user_id"], "5")
        self.assertEqual(result["events"][-1]["user_id"], "54")
        self.assertEqual(result["summary"], {"page_view": 50})

    def test_query_limits_are_bounded(self):
        for index in range(25):
            self.deliver({"event_type": "page_view", "user_id": str(index)})
        for limit, expected in (("0", 1), ("-5", 1), ("abc", 20), ("2", 2)):
            with self.subTest(limit=limit):
                self.assertEqual(self.client.get(f"/api/events?limit={limit}").json["total"], expected)

    def test_bad_events_and_empty_callbacks_do_not_break_feed(self):
        context = MagicMock()
        for event in (None, EventData("broken JSON"), EventData("[]"), EventData("null")):
            self.module._on_event(context, event)
        self.assertEqual(self.client.get("/api/events").json["total"], 0)
        self.deliver({"event_type": ["unexpected type"]})
        self.assertEqual(self.client.get("/api/events").json["summary"], {"unknown": 1})

    def test_missing_settings_prevent_import(self):
        for settings in ({"EVENT_HUB_CONNECTION_STR": ""}, {"EVENT_HUB_NAME": " "}):
            with self.subTest(settings=settings), patch.dict(os.environ, settings):
                with self.assertRaisesRegex(RuntimeError, "Azure Event Hubs is required"):
                    self.load_app("lab_missing_config")
        self.consumer_factory.assert_called_once()

    def test_malformed_configuration_has_clear_startup_error(self):
        self.consumer_factory.side_effect = ValueError("secret-sdk-detail")
        with self.assertRaisesRegex(RuntimeError, "Invalid Event Hubs configuration") as raised:
            self.load_app("lab_invalid_config")
        self.assertNotIn("secret-sdk-detail", str(raised.exception))

    def test_pages_are_served(self):
        for route in ("/", "/dashboard"):
            with self.client.get(route) as response:
                self.assertEqual(response.status_code, 200)


@unittest.skipIf(sys.platform == "win32", "Gunicorn runs on Linux/macOS")
class GunicornConfigurationTests(unittest.TestCase):
    def test_azure_style_command_loads_repository_configuration_automatically(self):
        from gunicorn.app.wsgiapp import WSGIApplication

        previous_directory = Path.cwd()
        self.addCleanup(os.chdir, previous_directory)
        os.chdir(APP_PATH.parent)
        with patch.object(sys, "argv", [
            "gunicorn", "--bind=0.0.0.0", "--timeout", "600", "app:app"
        ]):
            application = WSGIApplication()
        self.assertEqual(application.cfg.workers, 1)
        self.assertEqual(application.cfg.threads, 4)
        self.assertFalse(application.cfg.preload_app)


if __name__ == "__main__":
    unittest.main()
