import ballerina/io;
import ballerina/os;
import ballerina/uuid;
import ballerinax/kafka;

// Keep report totals from the event stream in memory for this CLI session.
type RestaurantStats record {
    string name;
    int orders = 0;
    decimal grossSales = 0.0d;
    int pricedOrders = 0;
};

map<RestaurantStats> restaurantStats = {};
int assignedDeliveries = 0;
int completedDeliveries = 0;
int onTimeDeliveries = 0;
int deliveriesWithOnTimeData = 0;
decimal totalDeliveryMinutes = 0.0d;
int deliveriesWithDuration = 0;

public function main() returns error? {
    // Read the broker address from the environment, with a local default.
    string kafkaUrl = os:getEnv("KAFKA_URL");
    if kafkaUrl == "" {
        // Use this repository's published Kafka port when running outside Docker.
        kafkaUrl = "localhost:29092";
    }
    string groupId = "admin" + uuid:createType1AsString();

    kafka:Consumer consumer = check new (kafkaUrl, {
        groupId,
        offsetReset: "earliest",
        topics: ["orders.created", "delivery.assigned", "delivery.completed"]
    });

    io:println("Admin reporting CLI connected to Kafka at ", kafkaUrl);
    io:println("Reading retained order and delivery events...");
    int initialEvents = check refreshReports(consumer);
    io:println("Loaded ", initialEvents, " event(s). New events are picked up when a report is requested.");

    boolean running = true;
    while running {
        printMenu();
        io:print("Choice: ");
        string|error input = io:readln();

        if input is error {
            io:println("Input closed. Shutting down the admin CLI.");
            break;
        }

        string choice = input.trim();
        if choice == "0" || choice == "exit" || choice == "quit" {
            running = false;
        } else if choice == "1" || choice == "restaurants" || choice == "restaurant-report" {
            int newEvents = check refreshReports(consumer);
            io:println("Read ", newEvents, " new event(s) from Kafka.");
            printRestaurantReport();
        } else if choice == "2" || choice == "deliveries" || choice == "delivery-report" {
            int newEvents = check refreshReports(consumer);
            io:println("Read ", newEvents, " new event(s) from Kafka.");
            printDeliveryReport();
        } else if choice == "3" || choice == "refresh" {
            int newEvents = check refreshReports(consumer);
            io:println("Read ", newEvents, " new event(s) from Kafka.");
        } else if choice == "help" {
            printHelp();
        } else {
            io:println("Unknown choice. Enter 1, 2, 3, help, or 0.");
        }
    }

    check consumer->close();
}

// Show the available report commands.
function printMenu() {
    io:println("\n Food Delivery Admin");
    io:println("1. Restaurant statistics");
    io:println("2. Delivery performance");
    io:println("3. Refresh Kafka events");
    io:println("help. Show command help");
    io:println("0. Exit");
}

// Explains the event fields used by the reports.
function printHelp() {
    io:println("Restaurant statistics count orders.created events by restaurant.");
    io:println("Delivery performance compares delivery.assigned with delivery.completed events.");
    io:println("Optional event fields: restaurantId, restaurantName, amount, durationMinutes, onTime.");
}

// Poll briefly until Kafka is idle, so reports include recently published records.
function refreshReports(kafka:Consumer consumer) returns int|error {
    int processed = 0;
    int emptyPolls = 0;
    int batches = 0;

    while emptyPolls < 2 && batches < 10 {
        kafka:BytesConsumerRecord[] records = check consumer->poll(0.5);
        batches += 1;

        if records.length() == 0 {
            emptyPolls += 1;
        } else {
            emptyPolls = 0;
            foreach kafka:BytesConsumerRecord kafkaRecord in records {
                processEvent(kafkaRecord);
                processed += 1;
            }
        }
    }
    return processed;
}

// Parse each Kafka payload and send it to the report matching its topic.
function processEvent(kafka:BytesConsumerRecord kafkaRecord) {
    string topic = kafkaRecord.offset.partition.topic;
    string|error payloadText = string:fromBytes(kafkaRecord.value);
    if payloadText is error {
        io:println("Skipped a non-UTF-8 event from ", topic, ".");
        return;
    }

    json|error event = payloadText.fromJsonString();
    if event is error {
        io:println("Skipped an invalid JSON event from ", topic, ".");
        return;
    }

    if topic == "orders.created" {
        recordRestaurantOrder(event);
    } else if topic == "delivery.assigned" {
        assignedDeliveries += 1;
    } else if topic == "delivery.completed" {
        recordCompletedDelivery(event);
    }
}

// Add a created order to its restaurant's totals.
function recordRestaurantOrder(json event) {
    string restaurantId = firstString(event, ["restaurantId", "restaurant_id", "restaurantName", "restaurant_name"], "unknown");
    string restaurantName = firstString(event, ["restaurantName", "restaurant_name", "restaurantId", "restaurant_id"], "Unknown restaurant");
    decimal? amount = firstDecimal(event, ["totalAmount", "amount", "total", "price"]);

    RestaurantStats? existing = restaurantStats.get(restaurantId);
    if existing is () {
        RestaurantStats created = {
            name: restaurantName,
            orders: 1,
            grossSales: amount ?: 0.0d,
            pricedOrders: amount is decimal ? 1 : 0
        };
        restaurantStats[restaurantId] = created;
    } else {
        existing.orders += 1;
        if existing.name == restaurantId && restaurantName != restaurantId {
            existing.name = restaurantName;
        }
        if amount is decimal {
            existing.grossSales += amount;
            existing.pricedOrders += 1;
        }
        restaurantStats[restaurantId] = existing;
    }
}

// Capture available timing fields from a completed delivery event.
function recordCompletedDelivery(json event) {
    completedDeliveries += 1;

    boolean? onTime = firstBoolean(event, ["onTime", "on_time"]);
    if onTime is boolean {
        deliveriesWithOnTimeData += 1;
        if onTime {
            onTimeDeliveries += 1;
        }
    }

    decimal? duration = firstDecimal(event, ["durationMinutes", "deliveryDurationMinutes", "deliveryTimeMinutes"]);
    if duration is decimal {
        totalDeliveryMinutes += duration;
        deliveriesWithDuration += 1;
    }
}

// Print restaurant order volume and any order values included in events.
function printRestaurantReport() {
    io:println("\n Restaurant Statistics ");
    if restaurantStats.length() == 0 {
        io:println("No restaurant orders have been observed yet.");
        return;
    }

    foreach string restaurantId in restaurantStats.keys() {
        RestaurantStats? stats = restaurantStats.get(restaurantId);
        if stats is RestaurantStats {
            string sales = stats.grossSales.toString();
            io:println(stats.name, " [", restaurantId, "] — orders: ", stats.orders,
                ", gross sales: N$", sales, " (", stats.pricedOrders, " priced order(s))");
        }
    }
    io:println("Restaurants reporting orders: ", restaurantStats.length());
}

// Print delivery completion, on-time, and duration metrics when provided.
function printDeliveryReport() {
    io:println("\n Delivery Performance ");
    io:println("Assigned deliveries: ", assignedDeliveries);
    io:println("Completed deliveries: ", completedDeliveries);

    if assignedDeliveries > 0 {
        decimal completionRate = (<decimal>completedDeliveries * 100.0d) / <decimal>assignedDeliveries;
        io:println("Completion rate: ", completionRate.toString(), "%");
    } else {
        io:println("Completion rate: N/A (no assignments observed)");
    }

    if deliveriesWithOnTimeData > 0 {
        decimal onTimeRate = (<decimal>onTimeDeliveries * 100.0d) / <decimal>deliveriesWithOnTimeData;
        io:println("On-time completion rate: ", onTimeRate.toString(), "% (",
            onTimeDeliveries, "/", deliveriesWithOnTimeData, ")");
    } else {
        io:println("On-time completion rate: N/A (events do not include an onTime field)");
    }

    if deliveriesWithDuration > 0 {
        decimal averageMinutes = totalDeliveryMinutes / <decimal>deliveriesWithDuration;
        io:println("Average delivery duration: ", averageMinutes.toString(), " minute(s)");
    } else {
        io:println("Average delivery duration: N/A (events do not include a duration in minutes)");
    }
}

// Return the first matching event field as a printable string.
function firstString(json event, string[] fieldNames, string fallback) returns string {
    if event is map<json> {
        foreach string fieldName in fieldNames {
            json? value = event.get(fieldName);
            if value is string || value is int || value is decimal || value is float {
                return value.toString();
            }
        }
    }
    return fallback;
}

// Read the first available numeric field from an event object.
function firstDecimal(json event, string[] fieldNames) returns decimal? {
    if event is map<json> {
        foreach string fieldName in fieldNames {
            json? value = event.get(fieldName);
            if value is int {
                return <decimal>value;
            } else if value is decimal {
                return value;
            } else if value is float {
                return <decimal>value;
            }
        }
    }
    return ();
}

// Read a boolean field without assuming every producer sends it.
function firstBoolean(json event, string[] fieldNames) returns boolean? {
    if event is map<json> {
        foreach string fieldName in fieldNames {
            json? value = event.get(fieldName);
            if value is boolean {
                return value;
            }
        }
    }
    return ();
}
