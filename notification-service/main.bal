import ballerina/io;
import ballerinax/kafka;

// listen for order, payment and delivery events 
kafka:ConsumerConfiguration consumerConfiguration = {
    groupId: "notification-service",
    topics: [
        "orders.created",
        "payments.completed",
        "delivery.assigned",
        "delivery.completed"
    ],
    offsetReset: kafka:OFFSET_RESET_EARLIEST
};

listener kafka:Listener notificationListener = new ("kafka:9092", consumerConfiguration);

service on notificationListener {
    remote function onConsumerRecord(kafka:Caller caller, kafka:BytesConsumerRecord[] records) returns error? {
        foreach kafka:BytesConsumerRecord consumerRecord in records {
            string payload = check string:fromBytes(consumerRecord.value);
            string topic = consumerRecord.offset.partition.topic;
            string message = string `${topic} event: ${payload}`;

            // Notify only the audiences affected by each event.
            match topic {
                "orders.created" => {
                    dispatchAlert("customer", message);
                    dispatchAlert("restaurant", message);
                }
                "payments.completed" => {
                    dispatchAlert("customer", message);
                    dispatchAlert("restaurant", message);
                }
                "delivery.assigned" => {
                    dispatchAlert("customer", message);
                    dispatchAlert("driver", message);
                }
                "delivery.completed" => {
                    dispatchAlert("customer", message);
                    dispatchAlert("restaurant", message);
                }
                _ => {
                    io:println(string `Ignoring unsupported event from ${topic}`);
                }
            }
        }
    }
}

// email, sms and push providers here.
function dispatchAlert(string audience, string message) {
    foreach string channel in ["email", "sms", "push"] {
        io:println(string `[notification] channel=${channel} audience=${audience} message=${message}`);
    }
}