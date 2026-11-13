(() => {
  "use strict";
  const $ = (id) => document.getElementById(id);
  const escape = (value) =>
    String(value).replace(
      /[&<>"']/g,
      (char) =>
        ({
          "&": "&amp;",
          "<": "&lt;",
          ">": "&gt;",
          '"': "&quot;",
          "'": "&#39;",
        })[char],
    );
  const money = (cents) =>
    cents == null
      ? "—"
      : new Intl.NumberFormat("en-CA", {
          style: "currency",
          currency: "CAD",
        }).format(cents / 100);
  const shortId = (id) => String(id).slice(0, 10);
  const stages = {
    shopping: "Shopping",
    checkout: "At checkout",
    purchased: "Purchased",
    unknown: "No cart snapshot",
  };
  const knownTypes = new Set([
    "page_view",
    "product_click",
    "add_to_cart",
    "purchase",
    "banner_click",
    "view_cart",
    "cart_update",
    "remove_from_cart",
    "begin_checkout",
    "checkout_exit",
  ]);
  let selectedUser = "",
    requestNumber = 0,
    timer,
    optionSignature = "";

  function userButton(userId, className = "shopper-button") {
    return `<button type="button" class="${className}" data-user="${escape(userId)}" title="${escape(userId)}">${escape(shortId(userId))}</button>`;
  }
  function statusPill(shopper) {
    const stageClass = shopper.active ? shopper.stage : "inactive";
    return `<span class="pill ${escape(stageClass)}">${escape(stages[shopper.stage] || "Unknown")}</span>${shopper.high_value_checkout ? '<span class="cell-sub" style="color:var(--gold)">High-value checkout</span>' : ""}`;
  }
  function activity(shopper) {
    if (!shopper.last_activity_at) return "—";
    const time = new Date(shopper.last_activity_at).toLocaleTimeString();
    return `<span title="${escape(shopper.last_activity_at)}">${escape(time)}</span><span class="cell-sub">${shopper.active ? '<i class="activity-dot"></i>Active' : "Inactive"}</span>`;
  }
  function renderConsumer(consumer) {
    const labels = {
      waiting: "WAITING FOR EVENTS",
      receiving: "EVENTS RECEIVED",
      error: "CONSUMER ERROR",
      stopped: "CONSUMER STOPPED",
    };
    const state = Object.hasOwn(labels, consumer?.state)
      ? consumer.state
      : "error";
    $("consumer-badge").className = `consumer-badge ${state}`;
    $("consumer-label").textContent = labels[state];
    $("consumer-message").textContent =
      (consumer?.message || "Consumer status is unavailable.") +
      (consumer?.last_received_at
        ? ` Last received at ${new Date(consumer.last_received_at).toLocaleTimeString()}.`
        : "");
  }
  function renderKPIs(data) {
    for (const [id, value] of Object.entries({
      "kpi-total": data.total,
      "kpi-views": data.summary.page_view,
      "kpi-cart": data.summary.add_to_cart,
      "kpi-purchase": data.summary.purchase,
      "kpi-active": data.active_shoppers,
      "kpi-high-value": data.high_value_checkouts,
    }))
      $(id).textContent = value ?? 0;
    const scope = selectedUser
      ? `shopper ${shortId(selectedUser)}`
      : "all shoppers";
    document.querySelectorAll(".metric-scope").forEach((element) => {
      element.textContent = scope;
    });
  }
  function renderShoppers(shoppers) {
    const focused = document.activeElement;
    const focusContainer = focused?.closest("#shopper-rows, #opportunities");
    const focusedUser = focused?.dataset.user;
    const signature = JSON.stringify(
      shoppers.map((shopper) => shopper.user_id).sort(),
    );
    if (signature !== optionSignature) {
      optionSignature = signature;
      const ids = shoppers.map((shopper) => shopper.user_id).sort();
      if (selectedUser && !ids.includes(selectedUser)) ids.push(selectedUser);
      $("user-filter").replaceChildren(
        new Option("All shoppers", ""),
        ...ids.map((id) => new Option(shortId(id), id)),
      );
      $("user-filter").value = selectedUser;
    }
    $("clear-filter").hidden = !selectedUser;
    $("shopper-rows").innerHTML = shoppers.length
      ? shoppers
          .map(
            (shopper) =>
              `<tr class="${shopper.user_id === selectedUser ? "selected" : ""} ${shopper.high_value_checkout ? "flagged" : ""}"><td><div class="user-cell"><span class="avatar" aria-hidden="true">${escape(shopper.user_id.slice(-2).toUpperCase())}</span>${userButton(shopper.user_id)}</div></td><td>${money(shopper.cart_total_cents)}<span class="cell-sub">${shopper.item_count == null ? "Snapshot unavailable" : `${shopper.item_count} item${shopper.item_count === 1 ? "" : "s"}`}</span></td><td>${statusPill(shopper)}</td><td>${activity(shopper)}</td><td>${shopper.observed_orders}</td><td>${money(shopper.observed_spend_cents)}</td></tr>`,
          )
          .join("")
      : '<tr><td colspan="6" class="empty">Waiting for shopper activity… Open the store to get started.</td></tr>';
    const flagged = shoppers.filter((shopper) => shopper.high_value_checkout);
    $("opportunities").innerHTML = flagged.length
      ? flagged
          .map(
            (shopper) =>
              `<button type="button" class="opportunity-card" data-user="${escape(shopper.user_id)}"><div><span class="shopper-link">${escape(shortId(shopper.user_id))}</span><span>${shopper.item_count} items · reviewing checkout</span></div><strong>${money(shopper.cart_total_cents)}</strong><b aria-hidden="true">↗</b></button>`,
          )
          .join("")
      : '<p class="empty">No active high-value checkouts. A shopper reviewing more than CAD $1,000 will appear here.</p>';
    if (focusContainer && focusedUser) {
      const replacement = [
        ...focusContainer.querySelectorAll("[data-user]"),
      ].find((button) => button.dataset.user === focusedUser);
      (replacement || $("user-filter")).focus({ preventScroll: true });
    }
  }
  function renderSelected(shopper) {
    $("selected-panel").hidden = !shopper;
    if (!shopper) return;
    $("selected-title").textContent = shopper.user_id;
    $("selected-stage").textContent = shopper.high_value_checkout
      ? "High-value checkout"
      : stages[shopper.stage];
    $("selected-stage").className =
      `pill ${shopper.high_value_checkout ? "gold" : "neutral"}`;
    $("selected-orders").textContent = shopper.observed_orders;
    $("selected-spend").textContent = money(shopper.observed_spend_cents);
    $("selected-cart").innerHTML = shopper.cart_items.length
      ? shopper.cart_items
          .map(
            (item) =>
              `<div class="cart-detail-line"><span>${escape(item.name)} <span class="cell-sub">Quantity ${item.quantity} · ${money(item.unit_price_cents)} each</span></span><strong>${money(item.line_total_cents)}</strong></div>`,
          )
          .join("") +
        `<div class="cart-detail-line"><strong>Subtotal CAD</strong><strong>${money(shopper.cart_total_cents)}</strong></div>`
      : `<p class="empty">${shopper.has_snapshot ? "The current cart is empty." : "This legacy event has no cart snapshot."}</p>`;
  }
  function renderChart(summary) {
    const entries = Object.entries(summary).sort((a, b) => b[1] - a[1]);
    const max = Math.max(1, ...Object.values(summary));
    $("bar-chart").innerHTML = entries.length
      ? entries
          .map(
            ([type, count]) =>
              `<div class="bar-row"><span class="bar-label">${escape(type)}</span><div class="bar-track"><div class="bar-fill" style="width:${(count / max) * 100}%"></div></div><span class="bar-count">${count}</span></div>`,
          )
          .join("")
      : '<p class="empty">No events in this window.</p>';
  }
  function describe(event) {
    const items = Array.isArray(event.cart_items) ? event.cart_items : [];
    const product =
      event.product_name ||
      items.find((item) => item?.product_id === event.product_id)?.name ||
      event.product_id ||
      "product";
    const descriptions = {
      page_view:
        event.page === "/checkout" ? "Viewed checkout" : "Visited the store",
      product_click: `Viewed ${product}`,
      add_to_cart: `Added ${product}`,
      view_cart: "Reviewed the cart",
      cart_update: `Changed quantity of ${product}`,
      remove_from_cart: `Removed ${product}`,
      begin_checkout: "Started checkout review",
      checkout_exit: "Returned to shopping",
      banner_click: "Explored the featured collection",
      purchase: `Placed demo order${event.order_id ? ` ${event.order_id}` : ""}`,
    };
    return (
      descriptions[event.event_type] ||
      String(event.event_type || "Unknown event")
    );
  }
  function renderFeed(events) {
    $("feed-title").textContent = selectedUser
      ? "Shopper activity timeline"
      : "Live event feed";
    $("feed-scope").textContent =
      `${selectedUser ? `Shopper ${shortId(selectedUser)}` : "All shoppers"} · newest received events first`;
    $("feed-count").textContent = `${events.length} / 50 RECENT`;
    $("feed").innerHTML = events.length
      ? [...events]
          .reverse()
          .map((event) => {
            const time = event.timestamp
              ? new Date(event.timestamp).toLocaleTimeString([], {
                  hour12: false,
                })
              : "—";
            const type =
              typeof event.event_type === "string"
                ? event.event_type
                : "unknown";
            const details = [
              event.page || "—",
              Number.isInteger(event.cart_total_cents)
                ? `${event.event_type === "purchase" ? "Order" : "Cart"} ${money(event.cart_total_cents)}`
                : "Legacy event",
            ];
            const user =
              typeof event.user_id === "string"
                ? userButton(event.user_id)
                : "anonymous";
            return `<article class="feed-row"><time class="feed-time">${escape(time)}</time><span class="feed-type ${knownTypes.has(type) ? type : ""}">${escape(type)}</span><div><p class="feed-description">${escape(describe(event))}</p><div class="feed-meta">${user} · ${escape(details.join(" · "))}</div></div></article>`;
          })
          .join("")
      : '<p class="empty">No recent activity for this selection.</p>';
  }
  async function refresh() {
    clearTimeout(timer);
    const request = ++requestNumber;
    try {
      const query = new URLSearchParams({ limit: "50" });
      if (selectedUser) query.set("user_id", selectedUser);
      const response = await fetch(`/api/events?${query}`, {
        signal: AbortSignal.timeout(10000),
        cache: "no-store",
      });
      if (!response.ok) throw new Error("Dashboard request failed");
      const data = await response.json();
      if (request !== requestNumber) return;
      renderConsumer(data.consumer);
      renderKPIs(data);
      renderShoppers(data.shoppers);
      renderSelected(data.selected_shopper);
      renderChart(data.summary);
      renderFeed(data.events);
      $("stale-message").hidden = true;
      $("last-refresh").textContent =
        `Updated ${new Date().toLocaleTimeString()}`;
    } catch {
      if (request === requestNumber) $("stale-message").hidden = false;
    } finally {
      if (request === requestNumber) timer = setTimeout(refresh, 2000);
    }
  }
  function selectUser(userId) {
    selectedUser = userId;
    $("user-filter").value = userId;
    refresh();
  }
  $("user-filter").addEventListener("change", (event) =>
    selectUser(event.target.value),
  );
  $("clear-filter").addEventListener("click", () => selectUser(""));
  document.addEventListener("click", (event) => {
    const target = event.target.closest("[data-user]");
    if (target) selectUser(target.dataset.user);
  });
  refresh();
})();
