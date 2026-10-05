import ballerina/http;
import ballerina/log;
import ballerina/os;
import ballerina/time;
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
final mongodb:Collection notifications = check collection("notifications");

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

function newestFirst(Notification[] items) returns Notification[] {
    Notification[] out = [];
    int i = items.length() - 1;
    while i >= 0 {
        out.push(items[i]);
        i -= 1;
    }
    return out;
}

public type Notification record {|
    string id;
    string topic;
    string message;
    string timestamp;
|};

// Notification HTTP Service: retrieve real-time alerts and audit trail.
@http:ServiceConfig {
    cors: {
        allowOrigins: ["*"]
    }
}
service /notifications on new http:Listener(9096) {

    // Retrieve all notifications (newest first).
    resource function get .() returns Notification[]|error {
        stream<Notification, error?> result = check notifications->find();
        Notification[] list = check from Notification n in result select n;
        return newestFirst(list);
    }

    // Filter notifications by Kafka topic.
    resource function get topic/[string topic]() returns Notification[]|error {
        stream<Notification, error?> result = check notifications->find({topic});
        Notification[] list = check from Notification n in result select n;
        return newestFirst(list);
    }
}

// Notification Service: listens to every lifecycle event and alerts users.
listener kafka:Listener allEvents = new (kafkaUrl, {
    groupId: "notification-service",
    topics: ["orders.created", "payments.completed", "delivery.assigned", "delivery.completed"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    pollingInterval: 1
});

service on allEvents {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string message = check string:fromBytes(rec.value);
            string topic = rec.offset.partition.topic;
            string text = string `[${topic}] ${message}`;

            Notification note = {
                id: prefixedId("notification"),
                topic,
                message,
                timestamp: time:utcToString(time:utcNow())
            };
            check notifications->insertOne(note);
            log:printInfo("NOTIFICATION " + text);
        }
    }
}
