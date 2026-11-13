# Week 10 Lab – Clickstream Analytics with Azure Event Hubs

**CST8916 – Remote Data and Real-time Applications**

---

## What You Will Build

A two-page web application deployed on **Azure App Service** that demonstrates real-time clickstream analytics using **Azure Event Hubs**.

### The Two Pages

| Page | URL | What it does |
|------|-----|-------------|
| Demo Store | `/` | A fake e-commerce site. Every user action sends an event to Event Hubs. |
| Live Dashboard | `/dashboard` | Polls `/api/events` every 2 seconds and displays a real-time analytics view. |

**Event Hubs is required.** The app refuses to start without a connection string. The dashboard shows only events received by the Event Hubs consumer; `/track` never writes directly to the dashboard buffer. A successful publish and a successful receive are separate steps.

---

## Architecture

```mermaid
flowchart LR
    subgraph AAS["☁️ Azure App Service"]
        direction TB
        Store["🛍️ client.html\nDemo Store\n(Event Producer)"]
        Dashboard["📊 dashboard.html\nLive Dashboard\n(Event Consumer View)"]

        subgraph Flask["⚙️ app.py — Flask Application"]
            direction TB
            Track["/track\nsend_to_event_hubs()"]
            API["/api/events\n_event_buffer"]
            Consumer["Background Consumer Thread\nEventHubConsumerClient"]
        end

        Store -->|"POST /track"| Track
        API -->|"JSON response"| Dashboard
        Dashboard -->|"polls every 2 s\nGET /api/events"| API
    end

    subgraph EH["☁️ Azure Event Hubs"]
        direction TB
        NS["Namespace: shopstream"]
        Hub["Event Hub: clickstream"]
        P0["Partition 0"]
        P1["Partition 1"]
        NS --> Hub
        Hub --> P0
        Hub --> P1
    end

    Track -->|"azure-eventhub SDK\npublish event"| EH
    EH -->|"read events\ncontinuously"| Consumer
    Consumer -->|"append to buffer"| API

    %% ── Colour palette ──────────────────────────────────────────────
    %% Frontend (warm amber)
    style Store     fill:#FFF3E0,stroke:#E65100,color:#212121,stroke-width:2px
    style Dashboard fill:#FFF3E0,stroke:#E65100,color:#212121,stroke-width:2px

    %% Flask internals (cool blue)
    style Flask     fill:#E3F2FD,stroke:#1565C0,color:#212121,stroke-width:2px
    style Track     fill:#BBDEFB,stroke:#1565C0,color:#212121,stroke-width:1.5px
    style API       fill:#BBDEFB,stroke:#1565C0,color:#212121,stroke-width:1.5px
    style Consumer  fill:#BBDEFB,stroke:#1565C0,color:#212121,stroke-width:1.5px

    %% App Service container (light grey)
    style AAS       fill:#F5F5F5,stroke:#616161,color:#212121,stroke-width:2px

    %% Event Hubs (purple / Microsoft brand)
    style EH        fill:#F3E5F5,stroke:#6A1B9A,color:#212121,stroke-width:2px
    style NS        fill:#E1BEE7,stroke:#6A1B9A,color:#212121,stroke-width:1.5px
    style Hub       fill:#CE93D8,stroke:#6A1B9A,color:#212121,stroke-width:1.5px
    style P0        fill:#BA68C8,stroke:#6A1B9A,color:#FFFFFF,stroke-width:1.5px
    style P1        fill:#BA68C8,stroke:#6A1B9A,color:#FFFFFF,stroke-width:1.5px
```

### Component Summary

| Component | Layer | Role | Technology |
|-----------|-------|------|------------|
| `client.html` — Demo Store | Frontend | Generates click events on every user interaction and sends them to the Flask backend | HTML + JavaScript (`fetch`) |
| `dashboard.html` — Live Dashboard | Frontend | Polls the backend every 2 seconds and renders a real-time event feed and bar chart | HTML + JavaScript (`setInterval`) |
| `/track` endpoint | Flask — Producer | Receives click events from the store, enriches them with a timestamp, and publishes them to Event Hubs | `EventHubProducerClient` |
| `/api/events` endpoint | Flask — API | Serves the in-memory event buffer as JSON to the dashboard on each poll | Flask route + `_event_buffer` list |
| Background Consumer Thread | Flask — Consumer | Continuously reads events from Event Hubs and appends them to the in-memory buffer | `EventHubConsumerClient` |
| Event Hubs Namespace (`shopstream`) | Azure Cloud | Top-level container that groups one or more Event Hubs under a single connection endpoint | Azure Event Hubs |
| Event Hub (`clickstream`) | Azure Cloud | The named channel that receives, stores, and delivers the stream of click events | Azure Event Hubs |
| Partition 0 / Partition 1 | Azure Cloud | Ordered sub-sequences within the Event Hub; events are distributed across partitions to enable parallel consumers and higher throughput | Azure Event Hubs |

---

## Why Azure Event Hubs?

The most obvious question when looking at this app is: **why not just write clicks directly to a database or keep them in memory?** Here is why Event Hubs is the right tool for this job.

### The problem without Event Hubs

Imagine the store page POSTs every click directly to a `/track` endpoint that writes to a database or an in-memory list. This works fine for one or two users, but it breaks down quickly:

| Scenario | What goes wrong |
|----------|----------------|
| 500 users clicking simultaneously | The database is hammered with 500+ writes/second — latency spikes, connections time out |
| The dashboard wants to read while writes are happening | Reads and writes compete for the same resource, slowing each other down |
| You want to add a second consumer (e.g., a fraud-detection service) | You have to change the producer code to fan out to two places — tight coupling |
| The consumer is temporarily offline | Events that arrived while it was down are lost forever |

### How Event Hubs solves this

Event Hubs acts as a **durable, high-throughput buffer** between producers and consumers:

```mermaid
flowchart LR
    A["🖱️ Browser clicks"]
    B["/track"]
    C[("☁️ Event Hubs\nthe buffer")]
    D["⚙️ Consumer thread"]
    E["📊 Dashboard"]

    A --> B --> C --> D --> E

    style A fill:#FFF3E0,stroke:#E65100,color:#212121
    style B fill:#BBDEFB,stroke:#1565C0,color:#212121
    style C fill:#E1BEE7,stroke:#6A1B9A,color:#212121
    style D fill:#BBDEFB,stroke:#1565C0,color:#212121
    style E fill:#FFF3E0,stroke:#E65100,color:#212121
```

- **Decoupling:** The producer (`/track`) only knows about Event Hubs — it does not care who reads the events or how many consumers there are. Add a second consumer (fraud detection, ML pipeline, audit log) without touching the producer.
- **Durability:** Events remain in Event Hubs for the configured retention period (1 day in this lab). This lab starts reading from the beginning of the retained stream after each app restart. Resuming from a saved position across restarts requires a persistent checkpoint store, which this lab does not configure.
- **Throughput:** Event Hubs is designed for millions of events per second. Partitions allow multiple consumers to read in parallel, distributing the load.
- **Backpressure isolation:** If the dashboard is slow or offline, Event Hubs absorbs the incoming events without slowing down the store page. The producer and consumer run at their own pace.

---

## Prerequisites

- Python 3.11 or later and pip installed
- An **Azure account** (free tier is fine)
- **Azure CLI** installed — [install guide](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)
- VS Code with the **REST Client** extension (optional)

---

## Part 1: Create the Azure Event Hubs Namespace

An **Event Hubs Namespace** is a container that holds one or more Event Hubs (similar to how a database server holds multiple databases).

### Step 1 – Log in to the Azure Portal

Go to [portal.azure.com](https://portal.azure.com) and sign in.

### Step 2 – Create a Resource Group

1. Search for **Resource groups** in the top search bar.
2. Click **Create**.
3. Fill in:
   - **Subscription:** your subscription
   - **Resource group name:** `cst8916-week10-rg`
   - **Region:** `Canada Central`
4. Click **Review + create** → **Create**.

### Step 3 – Create an Event Hubs Namespace

1. Search for **Event Hubs** in the top search bar.
2. Click **Create**.
3. Fill in:
   - **Subscription:** your subscription
   - **Resource group:** `cst8916-week10-rg`
   - **Namespace name:** `shopstream-<your-name>` (must be globally unique)
   - **Region:** `Canada Central`
   - **Pricing tier:** `Basic`
4. Click **Review + create** → **Create**.
5. Wait for deployment to complete, then click **Go to resource**.

### Step 4 – Create an Event Hub inside the Namespace

1. Inside your namespace, click **+ Event Hub** in the top toolbar.
2. Fill in:
   - **Name:** `clickstream`
   - **Partition count:** `2`
   - **Message retention:** `1` day
3. Click **Create**.

```
Event Hubs Namespace: shopstream-<your-name>
└── Event Hub: clickstream
    ├── Partition 0  ← some events land here
    └── Partition 1  ← other events land here
```

> **What is a partition?**
> A partition is an ordered sequence of events within an Event Hub. Events are distributed across partitions to allow multiple consumers to read in parallel. Think of it like two checkout lanes at a store — customers are split between them to keep things moving.

### Step 5 – Copy the Connection String

1. In the namespace, go to **Shared access policies** (left menu).
2. Click **RootManageSharedAccessKey**.
3. Copy the **Primary connection string** — you will need it in Part 2.

> **Security note:** A connection string contains a secret key. Never commit it to Git. You will store it as an environment variable.

---

## Part 2: Run the App Locally

### Step 1 – Clone and install

```bash
git clone https://github.com/YOUR-USERNAME/26F_CST8916_Week10-Event-Hubs-Lab.git
cd 26F_CST8916_Week10-Event-Hubs-Lab
pip install -r requirements.txt
```

### Step 2 – Set environment variables

**Linux / macOS:**
```bash
export EVENT_HUB_CONNECTION_STR="Endpoint=sb://shopstream-<your-name>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<your-key>"
export EVENT_HUB_NAME="clickstream"
```

**Windows (PowerShell):**
```powershell
$env:EVENT_HUB_CONNECTION_STR="Endpoint=sb://shopstream-<your-name>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<your-key>"
$env:EVENT_HUB_NAME="clickstream"
```

> Never put your connection string directly in the code. The app reads it from the environment so secrets stay out of source control.

The connection string must grant both **Send** and **Listen** access. The namespace-level `RootManageSharedAccessKey` used here grants both. `EVENT_HUB_NAME` defaults to `clickstream`; set it explicitly if you chose another name.

### Step 3 – Run the app

```bash
python app.py
```

You should see:
```
 * Running on http://0.0.0.0:8000
```

The consumer starts automatically when `app.py` loads. If the connection string is missing or malformed, startup stops with an explanatory error. Authentication, hub-name, and network problems appear in the dashboard status and application logs once the consumer tries to connect.

### Step 4 – Try it out

1. Open `http://localhost:8000` — the ShopStream store loads.
2. Click on products, add items to cart, click the banner.
3. Watch the **Event Hubs Publish Log** at the bottom of the store page. A confirmed publish shows `→ published to Event Hubs`; failed requests show an error.
4. Open `http://localhost:8000/dashboard`. It polls every 2 seconds and shows **WAITING FOR EVENTS** until an event is received, **EVENTS RECEIVED** with the last receive time, or **CONSUMER ERROR** if receiving fails.
5. Check that your clicks appear after the consumer receives them from Azure. Existing retained events may also appear when the app starts.

The dashboard counts the most recent 50 received events, rather than lifetime totals. Its buffer and consumer checkpoints are in memory. Restarting the app clears the buffer, then replays retained events. Replayed or redelivered events can appear again; this lab does not implement exactly-once processing.

---

## Part 3: Understanding the Code

### How an event travels from click to Event Hubs

```mermaid
flowchart TD
    S1["1️⃣ User clicks 'Add to Cart'\non the store page"]
    S2["2️⃣ JavaScript calls\ntrackEvent('add_to_cart', '/products/shoes', 'p_shoe_01')"]
    S3["3️⃣ fetch() sends POST /track\nwith JSON body:\nevent_type, page, product_id, user_id"]
    S4["4️⃣ Flask /track route enriches the event\n(adds timestamp)\nand calls send_to_event_hubs()"]
    S5["5️⃣ EventHubProducerClient\ncreates a batch and sends it\nto Azure Event Hubs"]
    S6["6️⃣ Event Hubs stores the event\nin one of its partitions"]
    S7["7️⃣ Background consumer thread\nreads the event via\nEventHubConsumerClient"]
    S8["8️⃣ _on_event() appends the event\nto the in-memory buffer"]
    S9["9️⃣ Dashboard polls GET /api/events\nand renders the updated\nevent feed and bar chart"]

    S1 --> S2 --> S3 --> S4 --> S5 --> S6 --> S7 --> S8 --> S9

    %% ── Colour palette ──────────────────────────────────
    %% Browser / Frontend (warm amber)
    style S1 fill:#FFF3E0,stroke:#E65100,color:#212121,stroke-width:2px
    style S2 fill:#FFF3E0,stroke:#E65100,color:#212121,stroke-width:2px
    style S3 fill:#FFF3E0,stroke:#E65100,color:#212121,stroke-width:2px

    %% Flask application logic (cool blue)
    style S4 fill:#BBDEFB,stroke:#1565C0,color:#212121,stroke-width:2px
    style S5 fill:#BBDEFB,stroke:#1565C0,color:#212121,stroke-width:2px

    %% Azure Event Hubs (purple)
    style S6 fill:#E1BEE7,stroke:#6A1B9A,color:#212121,stroke-width:2px

    %% Background consumer + buffer (cool blue)
    style S7 fill:#BBDEFB,stroke:#1565C0,color:#212121,stroke-width:2px
    style S8 fill:#BBDEFB,stroke:#1565C0,color:#212121,stroke-width:2px

    %% Dashboard / Frontend (warm amber)
    style S9 fill:#FFF3E0,stroke:#E65100,color:#212121,stroke-width:2px
```

### Key SDK classes (from `azure-eventhub`)

| Class | Role |
|-------|------|
| `EventHubProducerClient` | Sends events to Event Hubs |
| `EventHubConsumerClient` | Reads events from Event Hubs |
| `EventData` | Wraps a single event payload (bytes or string) |
| `create_batch()` | Groups multiple events into one efficient send operation |

### Event payload structure

```json
{
  "event_type": "add_to_cart",
  "page": "/products/shoes",
  "product_id": "p_shoe_01",
  "user_id": "u_4a2f",
  "session_id": "s_9b3e",
  "timestamp": "2026-03-18T14:22:05.123456+00:00"
}
```

---

## Part 4: Deploy to Azure App Service

You will deploy the app directly from your GitHub fork using Azure App Service's built-in GitHub integration — no CLI required.

### Step 1 – Fork the repository

1. Go to the lab repository on GitHub.
2. Click the **Fork** button at the top right.
3. This creates your own copy at `https://github.com/<your-username>/26F_CST8916_Week10-Event-Hubs-Lab`.

### Step 2 – Clone your fork locally

```bash
git clone https://github.com/<your-username>/26F_CST8916_Week10-Event-Hubs-Lab.git
cd 26F_CST8916_Week10-Event-Hubs-Lab
```

You already ran the app locally in Part 2 from this clone. Now you will deploy the same code to Azure directly from this GitHub repository.

### Step 3 – Create the Web App in the portal

1. Go to [portal.azure.com](https://portal.azure.com) and sign in.
2. In the top search bar, search for **App Services** and click it.
3. Click **+ Create** → **Web App**.
4. Fill in the **Basics** tab:

| Field | Value |
|-------|-------|
| **Subscription** | your subscription |
| **Resource group** | `cst8916-week10-rg` (same one from Part 1) |
| **Name** | `shopstream-<your-name>` (must be globally unique) |
| **Publish** | Code |
| **Runtime stack** | Python 3.11 |
| **Operating System** | Linux |
| **Region** | Canada Central |
| **Pricing plan** | Free F1 |

5. Click **Next: Deployment →**.

### Step 4 – Connect your GitHub fork

On the **Deployment** tab:

1. Set **Continuous deployment** to **Enable**.
2. Under **GitHub Actions settings**, click **Authorize** and sign in to GitHub when prompted.
3. Fill in:
   - **Organization:** your GitHub username
   - **Repository:** `26F_CST8916_Week10-Event-Hubs-Lab`
   - **Branch:** `main`
4. Click **Review + create** → **Create**.

> **What just happened?** Azure created a GitHub Actions workflow file in your repository (`.github/workflows/`). Every time you push to `main`, GitHub Actions automatically builds and redeploys the app to Azure. This is called **CI/CD — Continuous Integration / Continuous Deployment**.

### Step 5 – Set Application Settings (environment variables)

The app reads the Event Hubs connection string from environment variables. You set these in the portal so the secret never lives in your code or repository.

1. Go to your App Service → **Environment variables** in the left menu (under **Settings**).
2. Under the **App settings** tab, click **+ Add** and add each of the following:

| Name | Value |
|------|-------|
| `EVENT_HUB_CONNECTION_STR` | your connection string from Part 1, Step 5 |
| `EVENT_HUB_NAME` | `clickstream` |

3. Click **Apply** → **Confirm**.

> Setting secrets in Application Settings is the Azure equivalent of `export` on your local machine. The app reads them with `os.environ.get()` — the same code works in both environments without any changes.

### Automatic application startup

**Leave the Startup command field blank.** Azure App Service's Linux Python runtime automatically detects `app.py` and starts its Flask `app` object with Gunicorn. See [Microsoft's Flask startup documentation](https://learn.microsoft.com/en-us/azure/app-service/configure-language-python#flask-app).

Keep `app.py`, `requirements.txt`, and `gunicorn.conf.py` in the deployed repository root. Gunicorn automatically reads `gunicorn.conf.py`, which uses one worker with four request threads and disables application preloading. The Event Hubs consumer starts when that worker imports `app.py`. Locally, `python app.py` uses the same initialization.

This lab uses one App Service instance and one Gunicorn worker because its event buffer is in process memory. Scaling to multiple processes or instances requires shared storage for dashboard data.

If an earlier version of the lab has a custom Startup command configured, clear it once so Azure can use automatic startup.

### Step 6 – Verify the deployment

1. Go to your repository on GitHub → **Actions** tab.
2. You should see a workflow run in progress or completed. Click it to watch the build logs.
3. Once the workflow shows a green checkmark, go back to the Azure Portal.
4. In your App Service, click **Overview** → find the **Default domain** and click it.
5. The ShopStream store should load. Click around, then open `/dashboard` to see the live analytics.
6. Check the publish log and consumer status. A green GitHub Actions run verifies deployment; the dashboard receiving your events verifies the Event Hubs path. `/health` reports HTTP 503 if the consumer has reported an error or stopped.

> If the page does not load immediately, wait 30–60 seconds and refresh. The free tier can take a moment to cold-start.

### Step 7 – Make a change and redeploy

This step demonstrates the power of CI/CD.

1. Open `templates/client.html` in your local clone.
2. Change the banner text on the `<h2>` tag to something like `Welcome to ShopStream – Live on Azure!`.
3. Commit and push:

```bash
git add templates/client.html
git commit -m "update banner text"
git push
```

4. Go to GitHub → **Actions** tab and watch the deployment trigger automatically.
5. Refresh your live Azure URL — the change appears without any manual steps.

---

## Part 5: Observe Events in the Azure Portal

1. Go to your Event Hubs namespace in the Azure Portal.
2. Click on the `clickstream` Event Hub.
3. Select **Process data** → **Explore** to see incoming events in real time.

You can also view metrics:
- Go to the **Overview** tab of the namespace.
- Look at **Incoming Messages** and **Outgoing Messages** charts.

> These charts show the actual volume of events flowing through your Event Hub — the same data your app is producing.

### Verify the full Event Hubs path

1. Click **Add to Cart** in the store and confirm `published to Event Hubs` in the publish log.
2. Confirm the `add_to_cart` event appears on the dashboard and that its last receive time advances. Only the consumer can add an event to this feed.
3. Confirm **Incoming Messages** and **Outgoing Messages** increase for your Event Hub. Portal metrics may take time to refresh.
4. As a local failure check, stop the app, unset `EVENT_HUB_CONNECTION_STR`, and run `python app.py` again. It must refuse to start with an error naming the required setting. Restore the variable before continuing.
5. If publishing succeeds but no new events arrive, check **CONSUMER ERROR**, `/health`, and App Service **Log stream**. Verify the connection string has Listen permission as well as Send permission, and the Event Hub name matches.

## Run the regression checks

After installing `requirements.txt`, run:

```bash
python -m unittest discover -s tests -v
node tests/test_ui.cjs
```

The Python checks exercise Flask routes and automatic consumer startup with mocked Azure clients. The JavaScript checks exercise publish confirmations and dashboard error states with mocked HTTP responses. They do not require Azure credentials; Node.js is needed only for the JavaScript checks. Use the steps above for live Azure verification.

---

## Project Structure

```
26F_CST8916_Week10-Event-Hubs-Lab/
├── app.py               # Flask app: /track producer + /api/events consumer endpoint
├── gunicorn.conf.py     # Automatic Gunicorn settings for the single-worker lab
├── requirements.txt     # Python dependencies
├── .gitignore           # Excludes secrets and build artifacts
├── README.md            # This file
├── templates/
│   ├── client.html      # Demo e-commerce store (event producer)
│   └── dashboard.html   # Live analytics dashboard (event consumer view)
└── tests/               # Regression checks using mocked Azure and HTTP responses
```

---

## Reflection Questions

Answer these after completing the lab:

1. **Producers and consumers** — In this lab, what is the producer? What is the consumer? How are they decoupled from each other?

   > **Answer:** The **producer** is the Flask `/track` endpoint — it receives click events from the browser and publishes them to Event Hubs using `EventHubProducerClient`. The **consumer** is the background thread running `EventHubConsumerClient`, which reads events from Event Hubs and stores them in the in-memory buffer for the dashboard to display.
   >
   > They are decoupled because neither side knows about the other. The producer only talks to Event Hubs — it does not call the consumer directly. The consumer only reads from Event Hubs — it does not know which app produced the events or when. Event Hubs sits in the middle as a buffer, so the producer and consumer can run at different speeds and even go offline independently without breaking each other.

2. **Partitions** — The Event Hub has 2 partitions. If 1,000 events arrive per second, how does having 2 partitions help compared to having 1?

   > **Answer:** With 1 partition, all 1,000 events per second must flow through a single ordered sequence — only one consumer can read from it at a time, creating a bottleneck. With 2 partitions, Event Hubs splits the incoming events across both partitions, and two consumers can read in parallel — effectively doubling throughput. In general, more partitions allow more parallel consumers and higher overall throughput. This is why high-traffic systems use 32 or more partitions.

3. **Connection strings** — Why is it important to store the connection string as an environment variable rather than hardcoding it in `app.py`?

   > **Answer:** A connection string contains a secret key that grants full access to your Event Hubs namespace. If it is hardcoded in `app.py` and the file is pushed to GitHub, anyone who can see the repository — including the public if the repo is public — can use that key to read or write to your Event Hub, rack up costs, or delete data. Storing it as an environment variable keeps the secret out of source control entirely. The code stays safe to share, and the secret is only ever configured in the deployment environment (your terminal or Azure Application Settings).

4. **Event retention** — The Event Hub is configured to retain events for 1 day. What would happen if the consumer app was offline for 2 days and then restarted?

   > **Answer:** Events older than 1 day would be permanently deleted from Event Hubs before the consumer had a chance to read them. When the consumer restarts, it would only see events from the last 1 day — everything before that is gone. This means up to 1 day of click data would be lost. To prevent this, you could increase the retention period (up to 90 days on higher tiers), or use the **Capture** feature to automatically save all events to Azure Blob Storage as a backup before they expire.

5. **Scaling** — If this were a real store with 10,000 simultaneous users, what would you change about the architecture? (Hint: think about the in-memory buffer in `app.py`.)

   > **Answer:** The in-memory buffer (`_event_buffer`) in `app.py` has two major problems at scale. First, it only holds 50 events and resets every time the app restarts or a new instance starts — on App Service with multiple instances, each instance has its own separate buffer, so the dashboard would show incomplete data. Second, memory is lost on restart, meaning no historical data is preserved.
   >
   > At 10,000 simultaneous users the architecture should replace the in-memory buffer with a persistent store. A common approach is to feed Event Hubs into **Azure Stream Analytics**, which processes the stream and writes aggregated results to a database (e.g., Azure Cosmos DB or Azure SQL). The dashboard then queries the database instead of an in-memory list. This way, all app instances share the same data, and nothing is lost on restart.

---

## Cleanup (Important!)

To avoid charges after the lab, delete the resource group:

```bash
az group delete --name cst8916-week10-rg --yes --no-wait
```

This removes the Event Hubs namespace, the App Service, and all associated resources.
