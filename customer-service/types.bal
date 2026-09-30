import ballerina/constraint;

// ---------------------------------------------------------------------------
// Persisted documents (MongoDB)
// ---------------------------------------------------------------------------

// A delivery address, embedded inside the customer document.
public type Address record {|
    string id;
    // Friendly name such as "Home" or "Work"
    string label;
    string street;
    string city;
    string postalCode?;
    // Free-text directions for the driver (gate code, landmark, ...)
    string instructions?;
    float latitude?;
    float longitude?;
    boolean isDefault = false;
|};

// Customer account document (`customers` collection).
public type Customer record {|
    string id;
    string name;
    string email;
    string phone;
    Address[] addresses = [];
    string createdAt;
    string updatedAt;
|};

public type OrderItem record {
    string menuItemId?;
    string name;
    int quantity;
    decimal price;
};

public type StatusChange record {|
    string status;
    string at;
|};

// Read-model of a customer's order (`order_history` collection).
// Built from Kafka events published by the Order and Delivery services.
public type OrderHistory record {|
    string orderId;
    string customerId?;
    string restaurantId?;
    OrderItem[] items?;
    decimal totalAmount?;
    string deliveryAddressId?;
    string driverId?;
    string status;
    string createdAt?;
    string updatedAt;
    string deliveredAt?;
    StatusChange[] statusHistory?;
|};

// ---------------------------------------------------------------------------
// REST payloads (validated automatically by ballerina/http via constraints)
// ---------------------------------------------------------------------------

public type AddressInput record {|
    @constraint:String {minLength: 1, maxLength: 50}
    string label;
    @constraint:String {minLength: 1, maxLength: 200}
    string street;
    @constraint:String {minLength: 1, maxLength: 100}
    string city;
    string postalCode?;
    string instructions?;
    @constraint:Float {minValue: -90, maxValue: 90}
    float latitude?;
    @constraint:Float {minValue: -180, maxValue: 180}
    float longitude?;
    boolean isDefault = false;
|};

public type AddressUpdate record {|
    @constraint:String {minLength: 1, maxLength: 50}
    string label?;
    @constraint:String {minLength: 1, maxLength: 200}
    string street?;
    @constraint:String {minLength: 1, maxLength: 100}
    string city?;
    string postalCode?;
    string instructions?;
    @constraint:Float {minValue: -90, maxValue: 90}
    float latitude?;
    @constraint:Float {minValue: -180, maxValue: 180}
    float longitude?;
    boolean isDefault?;
|};

public type CustomerInput record {|
    @constraint:String {minLength: 1, maxLength: 100}
    string name;
    @constraint:String {pattern: re `^[^@\s]+@[^@\s]+\.[^@\s]+$`}
    string email;
    @constraint:String {pattern: re `^\+?[0-9 ]{7,20}$`}
    string phone;
    AddressInput[] addresses = [];
|};

public type CustomerUpdate record {|
    @constraint:String {minLength: 1, maxLength: 100}
    string name?;
    @constraint:String {pattern: re `^[^@\s]+@[^@\s]+\.[^@\s]+$`}
    string email?;
    @constraint:String {pattern: re `^\+?[0-9 ]{7,20}$`}
    string phone?;
|};

public type ErrorBody record {|
    string message;
|};

// ---------------------------------------------------------------------------
// Kafka event contracts consumed by this service.
// Open records so producers can add fields without breaking this consumer.
// ---------------------------------------------------------------------------

// Topic `orders.created` (published by Order Service).
public type OrderCreatedEvent record {
    string orderId;
    string customerId;
    string restaurantId?;
    OrderItem[] items = [];
    decimal totalAmount?;
    string deliveryAddressId?;
    string createdAt?;
};

// Topic `orders.updated` (published by Order Service on every state transition).
public type OrderStatusEvent record {
    string orderId;
    string customerId?;
    string status;
    string updatedAt?;
};

// Topic `delivery.completed` (published by Delivery Service).
public type DeliveryCompletedEvent record {
    string orderId;
    string customerId?;
    string driverId?;
    string deliveredAt?;
};
