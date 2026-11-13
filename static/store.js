/* A browser-owned cart; every tracked action carries a complete snapshot.
 * The server prices it, Event Hubs transports it, and the consumer projects it.
 */
(() => {
  "use strict";
  const $ = (id) => document.getElementById(id);
  const STORAGE_KEY = "shopstream.state.v1";
  const money = (cents) =>
    new Intl.NumberFormat("en-CA", {
      style: "currency",
      currency: "CAD",
    }).format(cents / 100);
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
  const newId = (prefix) => prefix + crypto.randomUUID().replaceAll("-", "");
  function read(storage, key) {
    try {
      return JSON.parse(window[storage].getItem(key));
    } catch {
      return null;
    }
  }
  let state = read("localStorage", STORAGE_KEY) || {};
  if (typeof state !== "object" || Array.isArray(state)) state = {};
  if (typeof state.userId !== "string") state.userId = newId("u_");
  if (!Number.isSafeInteger(state.sequence) || state.sequence < 0)
    state.sequence = 0;
  if (!Array.isArray(state.cart)) state.cart = [];
  let sessionId = read("sessionStorage", "shopstream.session");
  if (typeof sessionId !== "string") {
    sessionId = newId("s_");
    try {
      sessionStorage.setItem("shopstream.session", JSON.stringify(sessionId));
    } catch {
      /* Visit-only session. */
    }
  }
  let products = [],
    byId = {},
    busy = false,
    ready = false,
    toastTimer;
  let publishQueue = Promise.resolve();
  let previousPath = location.pathname;

  function persist() {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
    } catch {
      toast(
        "Browser storage is unavailable. Your cart will last for this visit.",
      );
    }
  }
  function toast(message) {
    $("toast").textContent = message;
    $("toast").classList.add("visible");
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => $("toast").classList.remove("visible"), 3500);
  }
  function cartItems() {
    return state.cart.map((item) => ({
      product_id: item.product_id,
      quantity: item.quantity,
    }));
  }
  function stage() {
    if (location.pathname !== "/checkout") return "shopping";
    return !state.cart.length && state.lastOrder ? "purchased" : "checkout";
  }
  function makeEvent(eventType, productId = null, extra = {}) {
    state.sequence += 1;
    const payload = {
      schema_version: 1,
      event_id: newId("e_"),
      sequence: state.sequence,
      event_type: eventType,
      page: location.pathname,
      product_id: productId,
      user_id: state.userId,
      session_id: sessionId,
      stage: stage(),
      cart_items: cartItems(),
      ...extra,
    };
    persist();
    return payload;
  }
  function logEvent(payload, success, message) {
    const log = $("event-log");
    log.querySelector("p")?.remove();
    const line = document.createElement("div");
    line.className = `log-line ${success ? "success" : "error"}`;
    line.textContent =
      `${new Date().toLocaleTimeString()} · ${payload.event_type}` +
      (payload.product_id
        ? ` · ${byId[payload.product_id]?.name || payload.product_id}`
        : "") +
      (success
        ? " → published to Event Hubs"
        : ` × publish not confirmed: ${message}`);
    log.append(line);
    while (log.children.length > 100) log.firstElementChild.remove();
    log.scrollTop = log.scrollHeight;
  }
  async function publish(payload) {
    try {
      const response = await fetch("/track", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(20000),
      });
      const data = await response.json();
      if (!response.ok || data.status !== "published") {
        throw new Error(data.error || "Publishing was not confirmed.");
      }
      logEvent(payload, true);
      return data.event;
    } catch (error) {
      logEvent(payload, false, error.message);
      throw error;
    }
  }
  function enqueue(payload) {
    const request = publishQueue.then(() => publish(payload));
    publishQueue = request.catch(() => {}); // A failure must not block later snapshots.
    return request;
  }
  function track(type, productId = null) {
    if (!ready || state.pendingPurchase) return;
    enqueue(makeEvent(type, productId)).catch(() => {
      toast("Activity publishing failed. Open Event activity for details.");
    });
  }
  function art(product, className = "") {
    return `<div class="${className} ${product.color}"><svg viewBox="0 0 240 200" aria-hidden="true"><use href="/static/products.svg#${product.art}"/></svg></div>`;
  }
  function total(items = state.cart) {
    return items.reduce(
      (sum, item) => sum + byId[item.product_id].price_cents * item.quantity,
      0,
    );
  }
  function lineItems(items, editable = false) {
    return items
      .map((item) => {
        const product = byId[item.product_id];
        const controls = editable
          ? `<div class="quantity"><button type="button" data-quantity="-1" data-id="${product.id}" aria-label="Decrease ${escape(product.name)} quantity" ${item.quantity === 1 ? 'title="Remove item"' : ""}>−</button><span aria-label="Quantity">${item.quantity}</span><button type="button" data-quantity="1" data-id="${product.id}" aria-label="Increase ${escape(product.name)} quantity" ${item.quantity >= 99 ? "disabled" : ""}>+</button></div><button type="button" class="remove" data-remove="${product.id}" aria-label="Remove ${escape(product.name)}">Remove</button>`
          : `<p class="line-meta">Quantity: ${item.quantity}</p>`;
        const lineTotal =
          item.line_total_cents ?? product.price_cents * item.quantity;
        return `<article class="line-item">${art(product, "line-art")}<div><h3 class="line-name">${escape(product.name)}</h3><p class="line-meta">${money(item.unit_price_cents ?? product.price_cents)} each</p>${controls}</div><strong class="line-price">${money(lineTotal)}</strong></article>`;
      })
      .join("");
  }
  function renderCatalog() {
    $("product-grid").innerHTML = products
      .map(
        (product) =>
          `<article class="product-card"><button type="button" class="product-open" data-product="${product.id}" aria-label="View ${escape(product.name)}">${art(product, "product-image")}<span class="product-category">${escape(product.category)}</span><span class="product-name">${escape(product.name)}</span></button><p class="product-description">${escape(product.description.split(". ")[0])}.</p><div class="product-bottom"><span class="price">${money(product.price_cents)}<small>CAD</small></span><button type="button" class="add-button" data-add="${product.id}" aria-label="Add ${escape(product.name)} to cart">Add to cart <span aria-hidden="true">+</span></button></div></article>`,
      )
      .join("");
  }
  function renderCart() {
    const count = state.cart.reduce((sum, item) => sum + item.quantity, 0);
    $("cart-count").textContent = count;
    $("drawer-count").textContent = `(${count})`;
    $("open-cart").setAttribute("aria-label", `Open cart, ${count} items`);
    $("cart-items").innerHTML = state.cart.length
      ? lineItems(state.cart, true)
      : '<div class="empty-cart"><span aria-hidden="true">◇</span><h3>A little room for something good.</h3><p>Your cart is empty. Find your next everyday favourite.</p></div>';
    $("cart-subtotal").textContent = money(total());
    $("go-checkout").disabled =
      !state.cart.length || Boolean(state.pendingPurchase);
  }
  function renderViews(focus = false) {
    const checkout = location.pathname === "/checkout";
    const confirmed =
      checkout && !state.cart.length && Boolean(state.lastOrder);
    $("store-view").hidden = checkout;
    $("checkout-view").hidden = !checkout || confirmed;
    $("confirmation-view").hidden = !confirmed;
    document.title = checkout
      ? `ShopStream · ${confirmed ? "Order confirmed" : "Checkout"}`
      : "ShopStream · Everyday, upgraded";
    if (confirmed) {
      $("order-number").textContent = state.lastOrder.order_id;
      $("confirmation-items").innerHTML = lineItems(state.lastOrder.cart_items);
      $("confirmation-total").textContent = money(
        state.lastOrder.cart_total_cents,
      );
    } else if (checkout) {
      $("checkout-items").innerHTML = state.cart.length
        ? lineItems(state.cart)
        : '<div class="empty-cart"><h3>Your cart is empty.</h3><p>Return to the collection to find something good.</p></div>';
      $("checkout-subtotal").textContent = money(total());
      $("checkout-total").textContent = money(total());
    }
    $("place-order").disabled = busy || !state.cart.length;
    $("place-order").textContent = busy
      ? "Confirming your order…"
      : state.pendingPurchase
        ? "Retry demo order →"
        : "Place demo order →";
    $("edit-cart").disabled = busy || Boolean(state.pendingPurchase);
    if (focus && checkout)
      $(confirmed ? "confirmation-title" : "checkout-title").focus();
    renderCart();
  }
  function navigate(path) {
    if (!ready) return;
    if (state.pendingPurchase && path !== "/checkout") {
      toast("Retry the pending order before changing your cart.");
      return;
    }
    const leavingCheckout =
      location.pathname === "/checkout" && path !== "/checkout";
    $("cart-dialog").close();
    $("product-dialog").close();
    if (location.pathname !== path) history.pushState({}, "", path);
    previousPath = path;
    renderViews(true);
    window.scrollTo(0, 0);
    if (leavingCheckout) track("checkout_exit");
    else if (path === "/checkout" && state.cart.length) track("begin_checkout");
    track("page_view");
  }
  function openCart() {
    if (!ready) return;
    if (state.pendingPurchase) {
      toast("Retry the pending order before changing your cart.");
      return;
    }
    if (location.pathname === "/checkout") navigate("/");
    renderCart();
    $("cart-dialog").showModal();
    track("view_cart");
  }
  function addToCart(id) {
    if (state.pendingPurchase || !ready) {
      toast("Retry your pending order first.");
      return;
    }
    const item = state.cart.find((item) => item.product_id === id);
    if (item?.quantity >= 99) {
      toast("You can add up to 99 of each item.");
      return;
    }
    if (item) item.quantity += 1;
    else state.cart.push({ product_id: id, quantity: 1 });
    state.lastOrder = null;
    persist();
    renderViews();
    $("product-dialog").close();
    track("add_to_cart", id);
    toast(`${byId[id].name} added to your cart.`);
  }
  function changeQuantity(id, delta) {
    if (state.pendingPurchase) return;
    const item = state.cart.find((item) => item.product_id === id);
    if (!item) return;
    item.quantity = Math.min(99, item.quantity + delta);
    const removed = item.quantity <= 0;
    if (removed)
      state.cart = state.cart.filter((item) => item.product_id !== id);
    persist();
    renderViews();
    track(removed ? "remove_from_cart" : "cart_update", id);
    // Re-rendering replaces controls; retain keyboard focus on a useful control.
    const next = $("cart-items").querySelector(
      `[data-quantity="${delta > 0 ? 1 : -1}"][data-id="${id}"]`,
    );
    (next || $("cart-dialog").querySelector("[data-close]")).focus();
    toast(
      removed
        ? `${byId[id].name} removed.`
        : `${byId[id].name} quantity: ${item.quantity}.`,
    );
  }
  function openProduct(id) {
    const product = byId[id];
    $("product-detail").innerHTML =
      `${art(product, "product-detail-art")}<div class="product-detail-copy"><span class="product-category">${escape(product.category)}</span><h2 id="product-dialog-title">${escape(product.name)}</h2><p>${escape(product.description)}</p><div class="product-bottom"><strong class="price">${money(product.price_cents)} <small>CAD</small></strong><button type="button" class="button primary" data-add="${id}">Add to cart <span aria-hidden="true">+</span></button></div></div>`;
    $("product-dialog").showModal();
    track("product_click", id);
  }
  async function placeOrder() {
    if (busy || !state.cart.length) return;
    if (!state.pendingPurchase) {
      state.pendingPurchase = makeEvent("purchase", null, {
        stage: "purchased",
        order_id: `SS-${crypto.randomUUID().slice(0, 13).toUpperCase()}`,
      });
      persist();
    }
    busy = true;
    $("purchase-status").textContent = "Waiting for confirmation…";
    renderViews();
    try {
      const event = await enqueue(state.pendingPurchase);
      state.lastOrder = {
        order_id: event.order_id,
        cart_items: event.cart_items,
        cart_total_cents: event.cart_total_cents,
      };
      state.cart = [];
      state.pendingPurchase = null;
      persist();
      $("purchase-status").textContent = "";
    } catch {
      $("purchase-status").textContent =
        "Publishing was not confirmed. Your cart is saved. Retry this order when the connection is available; it will keep the same order ID.";
    } finally {
      busy = false;
      renderViews(true);
    }
  }
  document.addEventListener("click", (event) => {
    const button = event.target.closest("button, a");
    if (!button) return;
    if (button.matches("[data-home], [data-return], [data-shop]")) {
      event.preventDefault();
      navigate("/");
      if (button.hasAttribute("data-shop")) $("collection").scrollIntoView();
    } else if (button.dataset.close) $(button.dataset.close).close();
    else if (button.dataset.add) addToCart(button.dataset.add);
    else if (button.dataset.product) openProduct(button.dataset.product);
    else if (button.dataset.quantity)
      changeQuantity(button.dataset.id, Number(button.dataset.quantity));
    else if (button.dataset.remove) changeQuantity(button.dataset.remove, -99);
  });
  $("open-cart").addEventListener("click", openCart);
  $("go-checkout").addEventListener("click", () => navigate("/checkout"));
  $("edit-cart").addEventListener("click", openCart);
  $("place-order").addEventListener("click", placeOrder);
  $("explore").addEventListener("click", () => {
    track("banner_click");
    $("collection").scrollIntoView({ behavior: "smooth" });
  });
  window.addEventListener("popstate", () => {
    if (!ready) return;
    if (state.pendingPurchase) {
      history.replaceState({}, "", "/checkout");
      renderViews();
      return;
    }
    const wasCheckout = previousPath === "/checkout";
    previousPath = location.pathname;
    $("cart-dialog").close();
    $("product-dialog").close();
    renderViews(true);
    if (wasCheckout && location.pathname !== "/checkout")
      track("checkout_exit");
    else if (location.pathname === "/checkout" && state.cart.length)
      track("begin_checkout");
    track("page_view");
  });
  for (const dialog of document.querySelectorAll("dialog")) {
    dialog.addEventListener("click", (event) => {
      if (event.target !== dialog) return;
      const rect = dialog.getBoundingClientRect();
      if (
        event.clientX < rect.left ||
        event.clientX > rect.right ||
        event.clientY < rect.top ||
        event.clientY > rect.bottom
      )
        dialog.close();
    });
  }
  async function init() {
    $("open-cart").disabled = true;
    try {
      const response = await fetch("/api/catalog");
      if (!response.ok) throw new Error("Catalogue unavailable");
      products = (await response.json()).products;
      byId = Object.fromEntries(
        products.map((product) => [product.id, product]),
      );
      const seen = new Set();
      state.cart = state.cart.filter((item) => {
        if (
          !item ||
          !byId[item.product_id] ||
          !Number.isInteger(item.quantity) ||
          item.quantity < 1 ||
          seen.has(item.product_id)
        )
          return false;
        item.quantity = Math.min(99, item.quantity);
        seen.add(item.product_id);
        return true;
      });
      if (state.pendingPurchase) {
        history.replaceState({}, "", "/checkout");
        previousPath = "/checkout";
        $("purchase-status").textContent =
          "Your previous order still needs confirmation. Retry to confirm the same order.";
      }
      persist();
      ready = true;
      $("shopper-id").textContent = state.userId.slice(0, 10);
      $("shopper-id").title = state.userId;
      renderCatalog();
      renderViews();
      $("open-cart").disabled = false;
      if (!state.pendingPurchase) {
        track("page_view");
        if (location.pathname === "/checkout" && state.cart.length)
          track("begin_checkout");
      }
    } catch {
      $("product-grid").innerHTML =
        '<p role="alert">The collection could not load. <button class="text-link" id="retry-catalog" type="button">Try again</button></p>';
      $("retry-catalog").addEventListener("click", init);
      toast("The collection could not load. Please try again.");
    }
  }
  init();
})();
