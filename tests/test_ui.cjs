/* Exercise the actual page scripts with a small DOM and HTTP test harness. */
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

function loadPage(template, responses) {
  class Element {
    constructor() {
      this.children = [];
      this.textContent = "";
      this.className = "";
      this.innerHTML = "";
    }
    appendChild(child) { this.children.push(child); }
    addEventListener() {}
    querySelector(selector) {
      if (selector === ".bar-label") return this.barLabel ||= new Element();
      return null;
    }
  }
  const elements = new Map();
  const calls = [];
  const context = vm.createContext({
    document: {
      getElementById(id) {
        if (!elements.has(id)) elements.set(id, new Element());
        return elements.get(id);
      },
      createElement: () => new Element(),
    },
    window: {location: {pathname: "/"}},
    console: {error() {}},
    setInterval() {},
    fetch: async (url, options) => {
      calls.push({url, options});
      const response = responses.shift();
      if (response instanceof Error) throw response;
      assert.ok(response, "Unexpected HTTP request");
      return {ok: response.ok, json: async () => response.body};
    },
  });
  const html = fs.readFileSync(path.join(__dirname, "../templates", template), "utf8");
  const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
  vm.runInContext(script, context);
  return {context, elements, calls};
}

const settle = () => new Promise(resolve => setImmediate(resolve));

test("store confirms only successful Event Hubs publishes", async () => {
  const page = loadPage("client.html", [{ok: true, body: {status: "published"}}]);
  await settle();
  assert.equal(page.calls[0].url, "/track");
  const status = page.elements.get("event-log").children[0].children[0];
  assert.equal(status.className, "ok");
  assert.match(status.textContent, /published to Event Hubs/);
});

test("store displays JSON HTTP errors as failures", async () => {
  const page = loadPage("client.html", [{ok: false, body: {error: "Event Hubs unavailable <test>"}}]);
  await settle();
  const status = page.elements.get("event-log").children[0].children[0];
  assert.equal(status.className, "err");
  assert.match(status.textContent, /publish not confirmed: Event Hubs unavailable <test>/);
  assert.equal(status.innerHTML, "");
});

test("store does not treat an arbitrary JSON response as a confirmed publish", async () => {
  const page = loadPage("client.html", [{ok: true, body: {status: "ok"}}]);
  await settle();
  assert.equal(page.elements.get("event-log").children[0].children[0].className, "err");
});

test("store displays network failures", async () => {
  const page = loadPage("client.html", [new Error("Network unavailable")]);
  await settle();
  assert.match(page.elements.get("event-log").children[0].children[0].textContent, /Network unavailable/);
});

test("dashboard reports consumer errors while showing already received events", async () => {
  const page = loadPage("dashboard.html", [{ok: true, body: {
    total: 1, summary: {page_view: 1},
    events: [{event_type: "page_view", page: "<external event>", user_id: "u_test"}],
    consumer: {state: "error", message: "Unable to receive events."},
  }}]);
  await settle();
  assert.equal(page.elements.get("consumer-label").textContent, "CONSUMER ERROR");
  assert.equal(page.elements.get("kpi-total").textContent, 1);
  const eventPage = page.elements.get("feed").children[0].children[2];
  assert.equal(eventPage.textContent, "<external event>");
  assert.equal(eventPage.innerHTML, "");
});

test("dashboard waits for proof of receiving events and reports last receive time", async () => {
  const page = loadPage("dashboard.html", [{ok: true, body: {
    total: 0, summary: {}, events: [],
    consumer: {state: "waiting", message: "Waiting to receive events."},
  }}]);
  await settle();
  assert.equal(page.elements.get("consumer-label").textContent, "WAITING FOR EVENTS");
  vm.runInContext('renderConsumerStatus({state: "receiving", message: "Events received.", last_received_at: "2026-11-13T12:00:00+00:00"})', page.context);
  assert.equal(page.elements.get("consumer-label").textContent, "EVENTS RECEIVED");
  assert.match(page.elements.get("consumer-message").textContent, /Last event received at/);
});

test("dashboard reports stale data on failed polls", async () => {
  for (const response of [{ok: false}, new Error("Network unavailable")]) {
    const page = loadPage("dashboard.html", [response]);
    await settle();
    assert.equal(page.elements.get("consumer-label").textContent, "CONSUMER ERROR");
    assert.match(page.elements.get("consumer-message").textContent, /may be out of date/);
  }
});
