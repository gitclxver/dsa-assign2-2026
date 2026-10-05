# 🚀 Quick Setup Guide: Distributed Food Delivery Platform

This project is an introductory **Distributed Systems** demonstration built with **Ballerina**, **Apache Kafka**, **MongoDB**, and **Docker**.

It demonstrates:
1. **Microservices Architecture**: Independent services with clear functional boundaries.
2. **Event-Driven Coordination**: Asynchronous order processing using Kafka pub/sub topics.
3. **Persistent State**: Relational and document state stored in MongoDB.
4. **Dynamic Surge Pricing**: Real-time pricing adjustment calculated based on active platform demand.

---

## 📋 Prerequisites

You only need two things installed on your machine:
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (must be running)
- [Python 3](https://www.python.org/) *(Uses only the Python standard library — no `pip install` required!)*

---

## ⚡ 3-Step Quickstart

### Step 1: Start All Services
From the root folder of the project, run:
```bash
docker compose up -d
```
> This starts MongoDB, Zookeeper, Kafka, automatically creates the Kafka topics via `kafka-init`, and boots all 7 Ballerina microservices.

To verify all containers are running:
```bash
docker compose ps
```

---

### Step 2: Seed Real-World Data
Run the lightweight Python seeder script to populate realistic restaurants, digital menus, customer profiles, and initial test orders:
```bash
python client/seed.py
```

You will see:
- 4 Restaurants added (*Luigi's Authentic Italian*, *Smash & Sizzle Burgers*, *Tokyo Ramen*, *Taqueria El Fuego*)
- 4 Customer accounts created with delivery addresses
- Sample orders placed and processed through the Kafka pipeline

---

### Step 3: View & Test the Application

You can explore the platform in three simple ways:

#### Option A: Visual Web Dashboard (Recommended)
Simply double-click or open **`client/html/index.html`** in any web browser!

- **🛒 Part 1 (Customer Portal)**: Browse restaurant menus, watch the live **Dynamic Surge Pricing** meter, build a cart, and track orders in real time.
- **🍳 Part 2 (Kitchen & Drivers)**: Advance food preparation status (`PREPARING`, `READY`) and click **"Mark Delivered ✅"** to trigger Kafka delivery completion.
- **📊 Part 3 (Admin Analytics)**: View live platform KPIs, total orders, completed deliveries, and restaurant revenue.

---

#### Option B: Terminal CLI
Run the unified Python CLI:
```bash
python client/cli.py
```
- Type `1` for the **Customer Portal** (browse menus, check surge rates, place orders, live-track order status).
- Type `2` for the **Admin Dashboard** (view lifetime orders, revenue per restaurant, delivery performance).

*Shortcut commands:*
```bash
python client/cli.py customer   # Jump directly to Customer Portal
python client/cli.py admin      # Jump directly to Admin Analytics
```

---

#### Option C: Manual REST Testing (Using Sample JSON Payloads)
Sample payloads are provided in the **`client/payloads/`** directory:

1. **Register a Customer**:
   ```bash
   curl -X POST http://localhost:9091/customers -H "Content-Type: application/json" -d @client/payloads/customer.json
   ```

2. **Add a Restaurant & Menu**:
   ```bash
   curl -X POST http://localhost:9092/restaurants -H "Content-Type: application/json" -d @client/payloads/restaurant.json
   ```

3. **Check Live Surge Multiplier**:
   ```bash
   curl http://localhost:9093/orders/surge
   ```

4. **Place an Order**:
   ```bash
   curl -X POST http://localhost:9093/orders -H "Content-Type: application/json" -d @client/payloads/order.json
   ```

5. **View Admin Reports**:
   ```bash
   curl http://localhost:9097/admin/reports/summary
   curl http://localhost:9097/admin/reports/restaurants
   curl http://localhost:9097/admin/reports/deliveries
   ```

---

## 🧠 Distributed Systems Concepts in Action

### 1. Asynchronous Event Pipeline (Kafka Choreography)
When an order is submitted:
1. `order-service` writes the order to MongoDB as `CREATED` and publishes an **`orders.created`** event to Kafka.
2. `payment-service` consumes **`orders.created`**, simulates payment approval, records the transaction, and publishes **`payments.completed`**.
3. `delivery-service` consumes **`payments.completed`**, assigns an available driver (e.g. *Alice*), marks the order as `OUT_FOR_DELIVERY`, and publishes **`delivery.assigned`**.
4. When a driver delivers the order, `delivery-service` receives a `PUT /deliveries/{orderId}/complete` request, records it, and emits **`delivery.completed`**.
5. `order-service` consumes **`delivery.completed`** and updates the order status to `DELIVERED`.
6. Meanwhile, `notification-service` listens to all topics and logs notifications.

### 2. Dynamic Surge Pricing
- **Formula**:
  $$\text{Active Load} = \text{Active Deliveries} + \text{Pending Orders}$$
- **Thresholds**:
  - Load $\ge 4$: **1.50x** (*HIGH* demand / driver shortage)
  - Load $\ge 2$: **1.25x** (*MEDIUM* demand)
  - Load $\ge 1$: **1.10x** (*LOW* demand)
  - Load $= 0$: **1.00x** (*NORMAL* demand)

---

## 🛑 How to Stop the Platform

To stop all containers and clear the database:
```bash
docker compose down -v
```
