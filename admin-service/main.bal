import ballerina/http;
import ballerina/os;
import ballerinax/mongodb;

function env(string name, string fallback) returns string {
    string value = os:getEnv(name);
    return value == "" ? fallback : value;
}

final string mongoUrl = env("MONGO_URL", "mongodb://localhost:27017");

final mongodb:Client mongo = check new ({connection: mongoUrl});
final mongodb:Collection orders = check collection("orders");
final mongodb:Collection deliveries = check collection("deliveries");

function collection(string name) returns mongodb:Collection|error {
    mongodb:Database db = check mongo->getDatabase("fooddelivery");
    return db->getCollection(name);
}

type Order record {
    string id;
    string customerId;
    string restaurantId;
    decimal total;
    string status;
};

type Delivery record {
    string id;
    string orderId;
    string driver;
    string status;
};

// Admin Service: reports on restaurants and delivery performance.
@http:ServiceConfig {
    cors: {
        allowOrigins: ["*"]
    }
}
service /admin on new http:Listener(9097) {

    // Orders count and revenue per restaurant.
    resource function get reports/restaurants() returns map<json>|error {
        stream<Order, error?> result = check orders->find();
        map<int> counts = {};
        map<decimal> revenue = {};
        check from Order o in result
            do {
                counts[o.restaurantId] = (counts[o.restaurantId] ?: 0) + 1;
                revenue[o.restaurantId] = (revenue[o.restaurantId] ?: 0d) + o.total;
            };
        return {orderCounts: counts.toJson(), revenue: revenue.toJson()};
    }

    // Delivery performance grouped by status.
    resource function get reports/deliveries() returns map<json>|error {
        stream<Delivery, error?> result = check deliveries->find();
        map<int> byStatus = {};
        check from Delivery d in result
            do {
                byStatus[d.status] = (byStatus[d.status] ?: 0) + 1;
            };
        return {deliveriesByStatus: byStatus.toJson()};
    }

    resource function get reports/summary() returns map<json>|error {
        int totalOrders = check orders->countDocuments();
        int totalDeliveries = check deliveries->countDocuments();
        return {totalOrders, totalDeliveries};
    }
}
