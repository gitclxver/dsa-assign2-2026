import ballerina/log;
import ballerina/os;
import ballerina/time;
import ballerinax/mongodb;

const DB_NAME = "customer_db";
const CUSTOMERS = "customers";
const ORDER_HISTORY = "order_history";

final string mongoUrl = envOr("MONGO_URL", "mongodb://mongodb:27017");

final mongodb:Client mongoClient = check new ({connection: mongoUrl});
final mongodb:Collection customers = check openCollection(CUSTOMERS);
final mongodb:Collection orderHistory = check openCollection(ORDER_HISTORY);

function init() returns error? {
    // Business keys are indexed so lookups are fast and duplicates are rejected by the database itself.
    check customers->createIndex({id: 1}, {unique: true});
    check customers->createIndex({email: 1}, {unique: true});
    check orderHistory->createIndex({orderId: 1}, {unique: true});
    check orderHistory->createIndex({customerId: 1, createdAt: -1});
    log:printInfo("Customer Service connected to MongoDB", url = mongoUrl, database = DB_NAME);
}

isolated function openCollection(string name) returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase(DB_NAME);
    return db->getCollection(name);
}

isolated function envOr(string name, string default) returns string {
    string value = os:getEnv(name);
    return value == "" ? default : value;
}

isolated function now() returns string => time:utcToString(time:utcNow());

// ---------------------------------------------------------------------------
// Customers
// ---------------------------------------------------------------------------

isolated function findCustomers() returns Customer[]|error {
    stream<Customer, error?> result = check customers->find({}, {sort: {createdAt: 1}});
    return from Customer c in result select c;
}

isolated function findCustomer(string id) returns Customer|error? {
    return customers->findOne({id});
}

isolated function findCustomerByEmail(string email) returns Customer|error? {
    return customers->findOne({email});
}

isolated function insertCustomer(Customer customer) returns error? {
    check customers->insertOne(customer);
}

// Applies `$set` to a customer. Returns false if the customer does not exist.
isolated function setCustomerFields(string id, map<json> fields) returns boolean|error {
    mongodb:UpdateResult result = check customers->updateOne({id}, {set: fields});
    return result.matchedCount > 0;
}

isolated function deleteCustomer(string id) returns boolean|error {
    mongodb:DeleteResult result = check customers->deleteOne({id});
    return result.deletedCount > 0;
}

// ---------------------------------------------------------------------------
// Order history (read model fed by Kafka)
// ---------------------------------------------------------------------------

isolated function findOrdersForCustomer(string customerId, string? status, int 'limit)
        returns OrderHistory[]|error {
    map<json> filter = {customerId};
    if status is string {
        filter["status"] = status;
    }
    stream<OrderHistory, error?> result =
        check orderHistory->find(filter, {sort: {createdAt: -1}, 'limit});
    return from OrderHistory o in result select o;
}

isolated function findOrder(string customerId, string orderId) returns OrderHistory|error? {
    return orderHistory->findOne({customerId, orderId});
}

// Idempotent: the same event delivered twice leaves the document unchanged.
isolated function recordOrderCreated(OrderCreatedEvent event) returns error? {
    string createdAt = event.createdAt ?: now();
    map<json> fields = {
        customerId: event.customerId,
        items: event.items.toJson(),
        createdAt,
        updatedAt: now()
    };
    if event.restaurantId is string {
        fields["restaurantId"] = event.restaurantId;
    }
    if event.totalAmount is decimal {
        fields["totalAmount"] = event.totalAmount;
    }
    if event.deliveryAddressId is string {
        fields["deliveryAddressId"] = event.deliveryAddressId;
    }
    // A status update may overtake the created event (different topics), so only
    // default the status to CREATED when this is the first time we see the order.
    _ = check orderHistory->updateOne({orderId: event.orderId}, {
        set: fields,
        setOnInsert: {status: "CREATED"},
        "addToSet": {statusHistory: {status: "CREATED", at: createdAt}}
    }, {upsert: true});
}

isolated function recordStatusChange(string orderId, string? customerId, string status, string at,
        map<json> extra = {}) returns error? {
    map<json> fields = {status, updatedAt: now()};
    foreach [string, json] [key, value] in extra.entries() {
        fields[key] = value;
    }
    if customerId is string {
        fields["customerId"] = customerId;
    }
    _ = check orderHistory->updateOne({orderId}, {
        set: fields,
        "addToSet": {statusHistory: {status, at}}
    }, {upsert: customerId is string});
}
