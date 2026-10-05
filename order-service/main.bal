import ballerina/http;
import ballerina/log;
import ballerina/os;
import ballerina/uuid;
import ballerinax/kafka;
import ballerinax/mongodb;

function env(string name, string fallback) returns string {
    string value = os:getEnv(name);
    return value == "" ? fallback : value;
}

final string mongoUrl = env("MONGO_URL", "mongodb://localhost:27017");
final string kafkaUrl = env("KAFKA_URL", "localhost:9092");

final mongodb:Client mongo = check new ({connection: mongoUrl});
final mongodb:Collection orders = check collection("orders");
final mongodb:Collection deliveries = check collection("deliveries");
final kafka:Producer orderProducer = check new (kafkaUrl);

function collection(string name) returns mongodb:Collection|error {
    mongodb:Database db = check mongo->getDatabase("fooddelivery");
    return db->getCollection(name);
}

function prefixedId(string prefix) returns string {
    string u = uuid:createType4AsString();
    string hex = "";
    int i = 0;
    while i < u.length() && hex.length() < 10 {
        string ch = u.substring(i, i + 1);
        if ch != "-" {
            hex += ch;
        }
        i += 1;
    }
    return string `${prefix}-${hex}`;
}

function newestFirst(Order[] items) returns Order[] {
    Order[] out = [];
    int i = items.length() - 1;
    while i >= 0 {
        out.push(items[i]);
        i -= 1;
    }
    return out;
}

function earliestFirst(Order[] items) returns Order[] {
    Order[] out = [];
    foreach Order o in items {
        out.push(o);
    }
    return out;
}

type Item record {|
    string name;
    int qty;
    decimal price;
|};

type Order record {
    string id;
    string customerId;
    string restaurantId;
    Item[] items;
    decimal baseTotal;
    decimal surgeMultiplier;
    decimal total;
    string status;
};

type NewOrder record {
    string customerId;
    string restaurantId;
    Item[] items;
};

type StatusEvent record {
    string orderId;
};

public type SurgeInfo record {|
    decimal multiplier;
    string demandLevel;
    int activeLoad;
    string reason;
|};

// Dynamic Pricing Model: calculates pricing multiplier based on demand and driver availability.
function calculateSurge() returns SurgeInfo|error {
    int activeDeliveries = check deliveries->countDocuments({status: "ASSIGNED"});
    int activeOrders = check orders->countDocuments({status: "CREATED"});
    int load = activeDeliveries + activeOrders;

    if load >= 4 {
        return {
            multiplier: 1.5d,
            demandLevel: "HIGH",
            activeLoad: load,
            reason: "Peak demand / low driver availability: 1.5x surge pricing applied"
        };
    } else if load >= 2 {
        return {
            multiplier: 1.25d,
            demandLevel: "MEDIUM",
            activeLoad: load,
            reason: "Moderate demand: 1.25x surge pricing applied"
        };
    } else if load >= 1 {
        return {
            multiplier: 1.1d,
            demandLevel: "LOW",
            activeLoad: load,
            reason: "Elevated demand: 1.1x surge pricing applied"
        };
    } else {
        return {
            multiplier: 1.0d,
            demandLevel: "NORMAL",
            activeLoad: load,
            reason: "Normal demand: standard pricing (1.0x)"
        };
    }
}

// Order Service: owns the order lifecycle state machine.
@http:ServiceConfig {
    cors: {
        allowOrigins: ["*"]
    }
}
service /orders on new http:Listener(9093) {

    // Dynamic surge pricing quote endpoint.
    resource function get surge() returns SurgeInfo|error {
        return check calculateSurge();
    }

    // Place an order as CREATED and publish orders.created with dynamic surge pricing.
    resource function post .(NewOrder input) returns Order|error {
        SurgeInfo surge = check calculateSurge();
        decimal base = 0d;
        foreach Item item in input.items {
            base = base + (item.price * <decimal>item.qty);
        }
        decimal mult = surge.multiplier;
        decimal finalTotal = base * mult;

        Order 'order = {
            id: prefixedId("order"),
            customerId: input.customerId,
            restaurantId: input.restaurantId,
            items: input.items,
            baseTotal: base,
            surgeMultiplier: mult,
            total: finalTotal,
            status: "CREATED"
        };
        check orders->insertOne('order);
        check orderProducer->send({topic: "orders.created", value: 'order.toJsonString().toBytes()});
        log:printInfo(string `Order ${'order.id} created with surge multiplier ${mult}x (Total: ${finalTotal})`);
        return 'order;
    }

    resource function get .(string? sort, string? status, string? restaurantId) returns Order[]|error {
        map<json> query = {};
        if status is string && status != "" {
            query["status"] = status;
        }
        if restaurantId is string && restaurantId != "" {
            query["restaurantId"] = restaurantId;
        }

        stream<Order, error?> result;
        if query.length() > 0 {
            result = check orders->find(query);
        } else {
            result = check orders->find();
        }
        Order[] list = check from Order o in result select o;
        if sort == "earliest" {
            return earliestFirst(list);
        }
        return newestFirst(list);
    }

    resource function get paid(string? restaurantId) returns Order[]|error {
        map<json> query = {status: "CONFIRMED"};
        if restaurantId is string && restaurantId != "" {
            query["restaurantId"] = restaurantId;
        }
        stream<Order, error?> result = check orders->find(query);
        Order[] list = check from Order o in result select o;
        return earliestFirst(list);
    }

    resource function get [string id]() returns Order|http:NotFound|error {
        Order? 'order = check orders->findOne({id});
        if 'order is () {
            return http:NOT_FOUND;
        }
        return 'order;
    }

    // Manual transitions used by the restaurant (PREPARING, READY, CANCELLED).
    resource function put [string id]/status(@http:Payload record {|string status;|} body)
            returns Order|http:NotFound|error {
        mongodb:UpdateResult result = check orders->updateOne({id}, {set: {status: body.status}});
        if result.matchedCount == 0 {
            return http:NOT_FOUND;
        }
        Order? 'order = check orders->findOne({id});
        return 'order ?: http:NOT_FOUND;
    }
}

// Advance the lifecycle from events produced by other services.
listener kafka:Listener orderEvents = new (kafkaUrl, {
    groupId: "order-service",
    topics: ["payments.completed", "delivery.assigned", "delivery.completed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    pollingInterval: 1
});

service on orderEvents {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string message = check string:fromBytes(rec.value);
            string topic = rec.offset.partition.topic;
            StatusEvent event = check (check message.fromJsonString()).cloneWithType();
            string status = topic == "payments.completed" ? "CONFIRMED"
                : topic == "delivery.assigned" ? "OUT_FOR_DELIVERY"
                : "DELIVERED";
            _ = check orders->updateOne({id: event.orderId}, {set: {status}});
            log:printInfo(string `Order ${event.orderId} is now ${status}`);
        }
    }
}
