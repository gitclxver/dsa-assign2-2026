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
final mongodb:Collection deliveries = check collection("deliveries");
final kafka:Producer deliveryProducer = check new (kafkaUrl);

// Simple pool of drivers to assign from.
final readonly & string[] drivers = ["Alice", "Bob", "Charlie", "Diana"];

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

function newestFirst(Delivery[] items) returns Delivery[] {
    Delivery[] out = [];
    int i = items.length() - 1;
    while i >= 0 {
        out.push(items[i]);
        i -= 1;
    }
    return out;
}

type Delivery record {|
    string id;
    string orderId;
    string driver;
    string status;
|};

type PaymentEvent record {
    string orderId;
};

// Delivery Service: assigns drivers and tracks delivery status.
@http:ServiceConfig {
    cors: {
        allowOrigins: ["*"]
    }
}
service /deliveries on new http:Listener(9095) {

    resource function get .() returns Delivery[]|error {
        stream<Delivery, error?> result = check deliveries->find();
        Delivery[] list = check from Delivery d in result select d;
        return newestFirst(list);
    }

    // Driver marks an order delivered and publishes delivery.completed.
    resource function put [string orderId]/complete() returns Delivery|http:NotFound|error {
        mongodb:UpdateResult result = check deliveries->updateOne({orderId}, {set: {status: "DELIVERED"}});
        if result.matchedCount == 0 {
            return http:NOT_FOUND;
        }
        json completed = {orderId};
        check deliveryProducer->send({topic: "delivery.completed", value: completed.toJsonString().toBytes()});
        log:printInfo(string `Delivery for order ${orderId} completed`);
        Delivery? delivery = check deliveries->findOne({orderId});
        return delivery ?: http:NOT_FOUND;
    }
}

// Assign a driver as soon as payment is confirmed.
listener kafka:Listener paymentsCompleted = new (kafkaUrl, {
    groupId: "delivery-service",
    topics: ["payments.completed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    pollingInterval: 1
});

service on paymentsCompleted {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string message = check string:fromBytes(rec.value);
            PaymentEvent event = check (check message.fromJsonString()).cloneWithType();

            int count = check deliveries->countDocuments();
            string driver = drivers[count % drivers.length()];
            Delivery delivery = {
                id: prefixedId("delivery"),
                orderId: event.orderId,
                driver,
                status: "ASSIGNED"
            };
            check deliveries->insertOne(delivery);

            json assigned = {orderId: event.orderId, driver};
            check deliveryProducer->send({topic: "delivery.assigned", value: assigned.toJsonString().toBytes()});
            log:printInfo(string `Driver ${driver} assigned to order ${event.orderId}`);
        }
    }
}
