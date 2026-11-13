"""Small, testable catalogue and consumer-side projection for the shopping lab.

Money is always integer CAD cents. Only the Event Hubs consumer calls consume();
HTTP publishing never updates these analytics directly.
"""

from collections import Counter, deque
from copy import deepcopy
from datetime import datetime, timezone

CATALOG = [
    {"id": "p_shoe_01", "name": "Trail Runners", "price_cents": 8999,
     "category": "Everyday", "art": "shoes", "color": "peach",
     "description": "Find your stride. Lightweight everyday runners with a cushioned sole and a little adventure built in."},
    {"id": "p_laptop_02", "name": "UltraBook Pro", "price_cents": 99900,
     "category": "Tech", "art": "laptop", "color": "blue",
     "description": "Big ideas, small footprint. A slim, capable notebook for your desk, your bag, and wherever work takes you."},
    {"id": "p_watch_03", "name": "SmartWatch X", "price_cents": 24999,
     "category": "Tech", "art": "watch", "color": "sage",
     "description": "A good day starts here. Keep your daily essentials close with a bright display and a comfortable everyday strap."},
    {"id": "p_camera_04", "name": "Pocket Camera", "price_cents": 34950,
     "category": "Tech", "art": "camera", "color": "sand",
     "description": "For the moments between plans. An easy-to-carry camera that makes room for a different point of view."},
    {"id": "p_headph_05", "name": "NoiseFree Buds", "price_cents": 12900,
     "category": "Tech", "art": "headphones", "color": "lavender",
     "description": "Your own little soundtrack. Comfortable wireless headphones for focused mornings and long-way-home afternoons."},
    {"id": "p_tshirt_06", "name": "Classic Tee", "price_cents": 2999,
     "category": "Everyday", "art": "shirt", "color": "rose",
     "description": "The one you reach for. A soft, easy-fitting essential that goes with just about everything."},
]
PRODUCTS = {product["id"]: product for product in CATALOG}
LEGACY_TYPES = {"page_view", "product_click", "add_to_cart", "purchase", "banner_click"}
VALID_EVENT_TYPES = LEGACY_TYPES | {
    "view_cart", "cart_update", "remove_from_cart", "begin_checkout", "checkout_exit",
}
STAGES = {"shopping", "checkout", "purchased"}
ACTIVE_SECONDS = 300
HIGH_VALUE_CENTS = 100000


def utc_now():
    return datetime.now(timezone.utc)


def parse_timestamp(value):
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else None
    except (ValueError, OverflowError):
        return None


def normalize_cart(items):
    """Validate a complete cart and price it using the server catalogue."""
    if not isinstance(items, list) or len(items) > len(CATALOG):
        raise ValueError("cart_items must be a list of catalogue products.")
    result, seen = [], set()
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("Each cart item must contain product_id and quantity.")
        product_id, quantity = item.get("product_id"), item.get("quantity")
        if not isinstance(product_id, str) or product_id not in PRODUCTS:
            raise ValueError("Unknown product in cart.")
        if product_id in seen:
            raise ValueError("Each product may appear only once in the cart.")
        if type(quantity) is not int or not 1 <= quantity <= 99:
            raise ValueError("Item quantities must be whole numbers from 1 to 99.")
        seen.add(product_id)
        product = PRODUCTS[product_id]
        result.append({"product_id": product_id, "name": product["name"],
                       "quantity": quantity, "unit_price_cents": product["price_cents"],
                       "line_total_cents": quantity * product["price_cents"]})
    return result


def normalize_event(payload, now=None):
    """Support the original simple events and the new versioned cart snapshots."""
    if not isinstance(payload, dict) or not payload:
        raise ValueError("Expected a JSON object containing a click event.")
    event_type = payload.get("event_type")
    if not isinstance(event_type, str) or event_type not in VALID_EVENT_TYPES:
        raise ValueError("Unsupported or missing event_type.")
    for field in ("page", "product_id", "user_id", "session_id"):
        value = payload.get(field)
        if value is not None and (not isinstance(value, str) or len(value) > 512):
            raise ValueError(f"{field} must be a string of at most 512 characters or null.")
    event = {"event_type": event_type, "page": payload.get("page", "/"),
             "product_id": payload.get("product_id"),
             "user_id": payload.get("user_id", "anonymous"),
             "session_id": payload.get("session_id"),
             "timestamp": (now or utc_now()).isoformat()}
    if "schema_version" not in payload:
        if event_type not in LEGACY_TYPES or "cart_items" in payload:
            raise ValueError("schema_version 1 is required for cart snapshots.")
        return event
    if type(payload["schema_version"]) is not int or payload["schema_version"] != 1:
        raise ValueError("Unsupported schema_version.")
    for field in ("event_id", "user_id", "session_id"):
        if not isinstance(payload.get(field), str) or not 1 <= len(payload[field]) <= 128:
            raise ValueError(f"{field} is required and must be at most 128 characters.")
    sequence = payload.get("sequence")
    if type(sequence) is not int or not 1 <= sequence <= 9007199254740991:
        raise ValueError("sequence must be a positive safe integer.")
    stage = payload.get("stage")
    if not isinstance(stage, str) or stage not in STAGES:
        raise ValueError("stage must be shopping, checkout, or purchased.")
    expected_stage = {"purchase": "purchased", "begin_checkout": "checkout",
                      "checkout_exit": "shopping"}.get(event_type)
    if expected_stage and stage != expected_stage:
        raise ValueError(f"{event_type} requires stage {expected_stage}.")
    if event["product_id"] is not None and event["product_id"] not in PRODUCTS:
        raise ValueError("Unknown product_id.")
    if event["product_id"] is not None:
        event["product_name"] = PRODUCTS[event["product_id"]]["name"]
    items = normalize_cart(payload.get("cart_items"))
    if event_type in {"purchase", "begin_checkout"} and not items:
        raise ValueError("Add an item before checking out.")
    event.update(schema_version=1, event_id=payload["event_id"], sequence=sequence,
                 stage=stage, cart_items=items,
                 cart_total_cents=sum(item["line_total_cents"] for item in items),
                 item_count=sum(item["quantity"] for item in items), currency="CAD")
    if event_type == "purchase":
        order_id = payload.get("order_id")
        if not isinstance(order_id, str) or not 1 <= len(order_id) <= 128:
            raise ValueError("purchase requires an order_id of at most 128 characters.")
        event["order_id"] = order_id
    return event


class ShopperAnalytics:
    """An in-memory projection. The caller holds the shared buffer lock."""

    def __init__(self, limit=50):
        self.limit = limit
        self.events = deque(maxlen=limit)
        self.shoppers = {}
        self.seen_events = set()
        self.seen_orders = set()

    def consume(self, event, transport_id=None):
        user_id = event.get("user_id")
        event_id = event.get("event_id")
        key = (("event", str(user_id), event_id) if isinstance(event_id, str)
               else ("transport", transport_id) if transport_id is not None else None)
        if key is not None and key in self.seen_events:
            return
        if key is not None:
            self.seen_events.add(key)
        self.events.append(event)
        if not isinstance(user_id, str) or not user_id:
            return
        shopper = self.shoppers.setdefault(user_id, {
            "user_id": user_id, "stage": "unknown", "last_activity_at": None,
            "sequence": 0, "has_snapshot": False, "cart_items": [],
            "cart_total_cents": None, "item_count": None,
            "observed_orders": 0, "observed_spend_cents": 0,
            "events": deque(maxlen=self.limit),
        })
        shopper["events"].append(event)
        timestamp = parse_timestamp(event.get("timestamp"))
        # Revalidate received data: a hub can also contain old or external messages.
        try:
            snapshot = normalize_event(event, timestamp) if timestamp else None
        except ValueError:
            snapshot = None
        if snapshot and snapshot.get("schema_version") == 1:
            if snapshot["event_type"] == "purchase":
                order_key = (user_id, snapshot["order_id"])
                if order_key not in self.seen_orders:
                    self.seen_orders.add(order_key)
                    shopper["observed_orders"] += 1
                    shopper["observed_spend_cents"] += snapshot["cart_total_cents"]
            if snapshot["sequence"] > shopper["sequence"]:
                shopper.update({field: snapshot[field] for field in (
                    "sequence", "stage", "cart_items", "cart_total_cents", "item_count")})
                shopper.update(has_snapshot=True, last_activity_at=timestamp.isoformat())
                if snapshot["event_type"] == "purchase":
                    shopper.update(cart_items=[], cart_total_cents=0, item_count=0)
        elif timestamp and not shopper["has_snapshot"]:
            previous = parse_timestamp(shopper["last_activity_at"])
            if previous is None or timestamp > previous:
                shopper["last_activity_at"] = timestamp.isoformat()

    def snapshot(self, limit=20, user_id=None, now=None):
        now = now or utc_now()
        rows = []
        for shopper in self.shoppers.values():
            row = deepcopy({key: value for key, value in shopper.items() if key != "events"})
            timestamp = parse_timestamp(row["last_activity_at"])
            row["active"] = timestamp is not None and 0 <= (now - timestamp).total_seconds() < ACTIVE_SECONDS
            row["high_value_checkout"] = bool(
                row["active"] and row["stage"] == "checkout"
                and row["cart_total_cents"] is not None
                and row["cart_total_cents"] > HIGH_VALUE_CENTS)
            rows.append(row)
        rows.sort(key=lambda row: (
            not row["high_value_checkout"], not row["active"],
            -(parse_timestamp(row["last_activity_at"]).timestamp() if row["last_activity_at"] else 0)))
        source = (self.shoppers.get(user_id, {}).get("events", [])
                  if user_id is not None else self.events)
        recent = list(source)[-limit:]
        counts = Counter(event.get("event_type") if isinstance(event.get("event_type"), str)
                         else "unknown" for event in recent)
        return {"events": deepcopy(recent), "summary": dict(counts), "total": len(recent),
                "shoppers": rows,
                "selected_shopper": next((row for row in rows if row["user_id"] == user_id), None),
                "active_shoppers": sum(row["active"] for row in rows),
                "high_value_checkouts": sum(row["high_value_checkout"] for row in rows)}
