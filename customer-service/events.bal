import ballerina/log;
import ballerinax/kafka;

const TOPIC_ORDERS_CREATED = "orders.created";
const TOPIC_ORDERS_UPDATED = "orders.updated";
const TOPIC_DELIVERY_COMPLETED = "delivery.completed";

final string kafkaUrl = envOr("KAFKA_URL", "kafka:9092");

// Every Customer Service replica joins the same consumer group, so the topic
// partitions are shared between replicas and each event is processed once.
listener kafka:Listener orderEventListener = new (kafkaUrl, {
    groupId: "customer-service",
    topics: [TOPIC_ORDERS_CREATED, TOPIC_ORDERS_UPDATED, TOPIC_DELIVERY_COMPLETED],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
});

isolated service on orderEventListener {
    remote function onConsumerRecord(kafka:BytesConsumerRecord[] records) {
        foreach kafka:BytesConsumerRecord rec in records {
            string topic = rec.offset.partition.topic;
            error? result = handleEvent(topic, rec.value);
            if result is error {
                // A malformed event is logged and skipped so it cannot block the partition.
                log:printError("Failed to process event", result, topic = topic, offset = rec.offset.offset);
            }
        }
    }

    remote function onError(kafka:Error kafkaError) {
        log:printError("Kafka listener error", kafkaError);
    }
}

isolated function handleEvent(string topic, byte[] value) returns error? {
    string payload = check string:fromBytes(value);
    match topic {
        TOPIC_ORDERS_CREATED => {
            OrderCreatedEvent event = check payload.fromJsonStringWithType();
            check recordOrderCreated(event);
            log:printInfo("Order added to customer history", orderId = event.orderId, customerId = event.customerId);
        }
        TOPIC_ORDERS_UPDATED => {
            OrderStatusEvent event = check payload.fromJsonStringWithType();
            check recordStatusChange(event.orderId, event.customerId, event.status, event.updatedAt ?: now());
            log:printInfo("Order status updated", orderId = event.orderId, status = event.status);
        }
        TOPIC_DELIVERY_COMPLETED => {
            DeliveryCompletedEvent event = check payload.fromJsonStringWithType();
            string deliveredAt = event.deliveredAt ?: now();
            map<json> extra = {deliveredAt};
            if event.driverId is string {
                extra["driverId"] = event.driverId;
            }
            check recordStatusChange(event.orderId, event.customerId, "DELIVERED", deliveredAt, extra);
            log:printInfo("Order marked delivered", orderId = event.orderId);
        }
    }
}
