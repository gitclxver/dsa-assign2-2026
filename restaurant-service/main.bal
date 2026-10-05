import ballerina/http;
import ballerina/os;
import ballerina/uuid;
import ballerinax/mongodb;

function env(string name, string fallback) returns string {
    string value = os:getEnv(name);
    return value == "" ? fallback : value;
}

final string mongoUrl = env("MONGO_URL", "mongodb://localhost:27017");

final mongodb:Client mongo = check new ({connection: mongoUrl});
final mongodb:Collection restaurants = check collection("restaurants");
final mongodb:Collection orders = check collection("orders");

function collection(string name) returns mongodb:Collection|error {
    mongodb:Database db = check mongo->getDatabase("fooddelivery");
    return db->getCollection(name);
}

function prefixedId(string prefix) returns string {
    return string `${prefix}-${uuid:createType1AsString().substring(0, 8)}`;
}

type MenuItem record {|
    string name;
    decimal price;
    int stock;
|};

type Restaurant record {|
    string id;
    string name;
    string openHours;
    MenuItem[] menu;
|};

type NewRestaurant record {|
    string name;
    string openHours;
    MenuItem[] menu;
|};

// Restaurant Service: digital menus, inventory and opening hours.
@http:ServiceConfig {
    cors: {
        allowOrigins: ["*"]
    }
}
service /restaurants on new http:Listener(9092) {

    resource function post .(NewRestaurant input) returns Restaurant|error {
        Restaurant restaurant = {id: prefixedId("restaurant"), ...input};
        check restaurants->insertOne(restaurant);
        return restaurant;
    }

    resource function get .() returns Restaurant[]|error {
        stream<Restaurant, error?> result = check restaurants->find();
        return from Restaurant r in result select r;
    }

    resource function get [string id]() returns Restaurant|http:NotFound|error {
        Restaurant? restaurant = check restaurants->findOne({id});
        if restaurant is () {
            return http:NOT_FOUND;
        }
        return restaurant;
    }

    resource function get [string id]/menu() returns MenuItem[]|http:NotFound|error {
        Restaurant? restaurant = check restaurants->findOne({id});
        if restaurant is () {
            return http:NOT_FOUND;
        }
        return restaurant.menu;
    }

    resource function put [string id](NewRestaurant input) returns Restaurant|http:NotFound|error {
        mongodb:UpdateResult result = check restaurants->updateOne({id}, {set: input});
        if result.matchedCount == 0 {
            return http:NOT_FOUND;
        }
        return {id, ...input};
    }

    resource function delete [string id]() returns http:NoContent|http:NotFound|error {
        mongodb:DeleteResult result = check restaurants->deleteOne({id});
        if result.deletedCount == 0 {
            return http:NOT_FOUND;
        }
        return http:NO_CONTENT;
    }

    // Retrieve paid orders for a restaurant (earliest first / FIFO priority).
    resource function get [string id]/orders(string? status) returns Order[]|error {
        map<json> filter = {restaurantId: id};
        if status is string && status != "" {
            filter["status"] = status;
        } else {
            filter["status"] = "CONFIRMED";
        }
        stream<Order, error?> result = check orders->find(filter);
        Order[] list = check from Order o in result select o;
        return earliestFirst(list);
    }
}

type Order record {
    string id;
    string customerId;
    string restaurantId;
    anydata items;
    decimal total;
    string status;
};

function earliestFirst(Order[] items) returns Order[] {
    Order[] out = [];
    foreach Order o in items {
        out.push(o);
    }
    return out;
}
