# Distributed Food Delivery Platform

A scalable, event-driven microservices food delivery platform built with **Ballerina**, **Apache Kafka**, **MongoDB**, and **Docker Compose**.

---

## 🏗 System Architecture & Services

The platform consists of 7 independent microservices interacting via REST and asynchronous Kafka events:

| Service | Port | Protocol | Role |
| :--- | :--- | :--- | :--- |
| **customer-service** | `9091` | HTTP / REST | Customer profiles, addresses, and order history lookup |
| **restaurant-service** | `9092` | HTTP / REST | Restaurant catalog, operating hours, and digital menus |
| **order-service** | `9093` | HTTP + Kafka | Order lifecycle state machine & **Dynamic Surge Pricing** |
| **payment-service** | `9094` | HTTP + Kafka | Processes payments, provides payment query API, and publishes completion |
| **delivery-service** | `9095` | HTTP + Kafka | Auto-assigns drivers, tracks dispatch, and delivers orders |
| **notification-service** | `9096` | HTTP + Kafka | Real-time event notifications for customers and restaurants with live feed API |
| **admin-service** | `9097` | HTTP / REST | Aggregates executive KPIs, restaurant revenue, and delivery stats |

### Asynchronous Kafka Event Workflow
```
[Order Service]  -- (orders.created) ------>  [Payment Service]
                                                     |
                                            (payments.completed)
                                                     v
[Admin / Notify] <---- (delivery.assigned) <-- [Delivery Service]
                                                     |
                                            (delivery.completed)
                                                     v
                                              [Order Service] (Marks DELIVERED)
```

---

## ⚡ Dynamic Surge Pricing Model

The platform implements real-time demand-responsive pricing:
- **Active Load** = Count of active deliveries (`ASSIGNED`) + active orders (`CREATED`, `CONFIRMED`, `PREPARING`, `READY`, `OUT_FOR_DELIVERY`).
- **Tiers**:
  - `Load >= 4` : **1.50x** (*HIGH* demand / driver shortage)
  - `Load >= 2` : **1.25x** (*MEDIUM* demand)
  - `Load >= 1` : **1.10x** (*LOW* demand)
  - `Load == 0` : **1.00x** (*NORMAL* demand)
- **Endpoint**: `GET http://localhost:9093/orders/surge` returns live multiplier, demand level, and load reason.

---

## 🚀 How to Run the Platform

### 1. Start all Services via Docker Compose
Make sure Docker Desktop is running, then execute:
```bash
docker compose up -d
```
> All 7 microservices, Kafka, Zookeeper, and MongoDB will build and start in detached mode.

To view logs across all services:
```bash
docker compose logs -f
```

---

## 🥑 Populate Real-World Seed Data

To populate the database with realistic restaurants (Luigi's Italian, Smash Burgers, Tokyo Ramen, Taqueria El Fuego), digital menus, customer profiles, and sample orders:

```bash
python client/seed.py
```
*(Uses only Python's standard library — no pip packages needed).*

---

## 🖥 3-Part Interactive Web Client (HTML Dashboard)

A responsive 3-part web application is included in `client/html/index.html`.

### How to Open:
Simply open **`client/html/index.html`** in any modern web browser (or double-click the file in File Explorer).

### The 3 Portals:
1. **🛒 Customer Portal**:
   - Select customer profile and delivery address.
   - Explore restaurant menus with real-time stock and prices.
   - Live **Dynamic Surge Pricing Meter** badge.
   - Interactive cart with automatic surge multiplier price calculation.
   - Real-time order lifecycle tracker (`CREATED` $\rightarrow$ `CONFIRMED` $\rightarrow$ `OUT_FOR_DELIVERY` $\rightarrow$ `DELIVERED`).
2. **🍳 Kitchen & Driver Operations**:
   - **Kitchen Board**: Advance order status manually (`PREPARING`, `READY`).
   - **Driver Dispatch**: View assigned orders and click **"Mark Delivered ✅"** to trigger the Kafka completion cascade.
3. **📊 Admin Analytics & Reports**:
   - Executive KPIs: Total Lifetime Orders, Active Deliveries, System Revenue.
   - Restaurant Sales & Revenue breakdown table.
   - Delivery performance metrics.

---

## 💻 Terminal CLI Client

A clean, interactive command-line client is located at **`client/cli.py`**:

```bash
python client/cli.py
```
- Select `1` for the **Customer Portal** (browse menus, check surge price, place orders, live-track status).
- Select `2` for the **Admin Dashboard** (view KPIs, restaurant revenue, delivery performance, simulate driver delivery).

Direct shortcuts:
```bash
python client/cli.py customer   # Jump directly to Customer Portal
python client/cli.py admin      # Jump directly to Admin Analytics
```

---

## 📦 Sample JSON Payloads

Ready-to-use JSON payloads are provided in **`client/payloads/`** for testing via `curl` or Postman:
- `client/payloads/customer.json` — Register a customer
- `client/payloads/restaurant.json` — Add a restaurant with menu items
- `client/payloads/order.json` — Place an order
- `client/payloads/order_status.json` — Update order status (e.g. `PREPARING`)

---

## 🛑 Stopping the Platform

To stop and remove all containers and data volumes:
```bash
docker compose down -v
```
