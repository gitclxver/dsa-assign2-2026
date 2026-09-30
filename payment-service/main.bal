import ballerinax/kafka;
import ballerina/os;
import ballerina/uuid;
import ballerina/time;
import ballerina/log;
import ballerina/random;

// Incoming event on `orders.created` (same fields as customer-service/types.bal).
// Open record so extra fields from the Order Service don't break us.
type OrderCreated record {
    string orderId;
    string customerId;
    string restaurantId?;
    decimal totalAmount;
};

// Outgoing event on `payments.completed`.
type PaymentEvent record {|
    string paymentId;
    string orderId;
    string customerId;
    string? restaurantId;
    decimal amount;
    string status; // "SUCCESS" or "FAILED"
    string timestamp;
|};

string kafkaUrl = os:getEnv("KAFKA_URL") == "" ? "localhost:29092" : os:getEnv("KAFKA_URL");

final kafka:Producer producer = check new (kafkaUrl);

listener kafka:Listener orderListener = new(kafkaUrl, {
    groupId: "payment-service",
    topics: ["orders.created"]
});

service on orderListener {
    remote function onConsumerRecord(OrderCreated[] orders) returns error? {
        foreach OrderCreated o in orders {
            // 1. simulate payment: invalid amount fails, otherwise ~90% succeed
            string status = "SUCCESS";
            if o.totalAmount <= 0d {
                status = "FAILED";
            } else if check random:createIntInRange(0, 10) == 0 {
                status = "FAILED";
            }

            // 2. build the event
            PaymentEvent event = {
                paymentId: uuid:createType4AsString(),
                orderId: o.orderId,
                customerId: o.customerId,
                restaurantId: o.restaurantId,
                amount: o.totalAmount,
                status: status,
                timestamp: time:utcToString(time:utcNow())
            };

            // 3. publish, keyed by orderId so one order's events stay in one partition
            check producer->send({topic: "payments.completed", key: o.orderId, value: event});

            // 4. log it
            log:printInfo("Payment processed", orderId = o.orderId, status = status, amount = o.totalAmount);
        }
    }
}