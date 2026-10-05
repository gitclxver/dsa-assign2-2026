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
final mongodb:Collection payments = check collection("payments");
final kafka:Producer paymentProducer = check new (kafkaUrl);

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

function newestFirst(Payment[] items) returns Payment[] {
    Payment[] out = [];
    int i = items.length() - 1;
    while i >= 0 {
        out.push(items[i]);
        i -= 1;
    }
    return out;
}

function asDecimal(json? value) returns decimal {
    if value is decimal {
        return value;
    }
    if value is float {
        return <decimal>value;
    }
    if value is int {
        return <decimal>value;
    }
    if value is string {
        decimal|error parsed = decimal:fromString(value);
        if parsed is decimal {
            return parsed;
        }
    }
    return 0d;
}

public type Payment record {|
    string id;
    string orderId;
    decimal amount;
    string status;
|};

public type PayRequest record {|
    string orderId;
    decimal amount?;
|};

type OrderEvent record {
    string id;
    decimal total;
};

type OrderLookup record {
    string id;
    decimal total;
};

function processPayment(string orderId, decimal? inputAmount) returns Payment|error {
    if orderId == "" {
        return error("orderId is required");
    }

    decimal finalAmount = 0d;
    if inputAmount is decimal && inputAmount > 0d {
        finalAmount = inputAmount;
    } else {
        Payment? pending = check payments->findOne({orderId: orderId});
        if pending is Payment && pending.amount > 0d {
            finalAmount = pending.amount;
        } else {
            mongodb:Collection ordersColl = check collection("orders");
            OrderLookup? ord = check ordersColl->findOne({id: orderId});
            if ord is OrderLookup {
                finalAmount = ord.total;
            } else {
                map<json>? raw = check ordersColl->findOne({id: orderId});
                if raw is map<json> {
                    finalAmount = asDecimal(raw["total"]);
                }
            }
        }
    }

    if finalAmount <= 0d {
        return error(string `Cannot pay order ${orderId}: amount unknown or zero`);
    }

    Payment payment = {
        id: prefixedId("payment"),
        orderId: orderId,
        amount: finalAmount,
        status: "PAID"
    };

    _ = check payments->deleteOne({orderId: orderId});
    check payments->insertOne(payment);

    json completed = {orderId: orderId, amount: payment.amount, status: "PAID"};
    check paymentProducer->send({topic: "payments.completed", value: completed.toJsonString().toBytes()});
    log:printInfo(string `Payment ${payment.id} confirmed for order ${orderId} ($${payment.amount})`);
    return payment;
}

@http:ServiceConfig {
    cors: {
        allowOrigins: ["*"]
    }
}
service /payments on new http:Listener(9094) {

    resource function get .() returns Payment[]|error {
        stream<Payment, error?> result = check payments->find();
        Payment[] list = check from Payment p in result select p;
        return newestFirst(list);
    }

    resource function get [string idOrOrderId]() returns Payment|http:NotFound|error {
        Payment? p = check payments->findOne({orderId: idOrOrderId});
        if p is () {
            Payment? byId = check payments->findOne({id: idOrOrderId});
            if byId is () {
                return http:NOT_FOUND;
            }
            return byId;
        }
        return p;
    }

    resource function post pay(@http:Payload PayRequest input) returns Payment|error {
        return processPayment(input.orderId, input.amount);
    }

    resource function post .(@http:Payload PayRequest input) returns Payment|error {
        return processPayment(input.orderId, input.amount);
    }
}

listener kafka:Listener orderCreated = new (kafkaUrl, {
    groupId: "payment-service",
    topics: ["orders.created"],
    offsetReset: kafka:OFFSET_RESET_EARLIEST,
    pollingInterval: 1
});

service on orderCreated {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord rec in records {
            string message = check string:fromBytes(rec.value);
            OrderEvent event = check (check message.fromJsonString()).cloneWithType();

            Payment? existing = check payments->findOne({orderId: event.id});
            if existing is () {
                Payment pending = {
                    id: prefixedId("payment"),
                    orderId: event.id,
                    amount: event.total,
                    status: "PENDING"
                };
                check payments->insertOne(pending);
                log:printInfo(string `Order ${event.id} registered awaiting payment ($${event.total})`);
            }
        }
    }
}
