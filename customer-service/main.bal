import ballerina/http;
import ballerina/os;
import ballerina/uuid;
import ballerinax/mongodb;

// Read config from the environment (docker) or fall back to localhost.
function env(string name, string fallback) returns string {
    string value = os:getEnv(name);
    return value == "" ? fallback : value;
}

final string mongoUrl = env("MONGO_URL", "mongodb://localhost:27017");

final mongodb:Client mongo = check new ({connection: mongoUrl});
final mongodb:Collection customers = check collection("customers");
final mongodb:Collection orders = check collection("orders");

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

function newestFirst(json[] items) returns json[] {
    json[] out = [];
    int i = items.length() - 1;
    while i >= 0 {
        out.push(items[i]);
        i -= 1;
    }
    return out;
}

type Customer record {|
    string id;
    string name;
    string email;
    string phone;
    string[] addresses;
|};

type NewCustomer record {|
    string name;
    string email;
    string phone;
    string[] addresses;
|};

// Customer Service: accounts, delivery addresses and order history.
@http:ServiceConfig {
    cors: {
        allowOrigins: ["*"]
    }
}
service /customers on new http:Listener(9091) {

    resource function post .(NewCustomer input) returns Customer|error {
        Customer customer = {id: prefixedId("customer"), ...input};
        check customers->insertOne(customer);
        return customer;
    }

    resource function get .() returns Customer[]|error {
        stream<Customer, error?> result = check customers->find();
        return from Customer c in result select c;
    }

    resource function get [string id]() returns Customer|http:NotFound|error {
        Customer? customer = check customers->findOne({id});
        if customer is () {
            return http:NOT_FOUND;
        }
        return customer;
    }

    resource function put [string id](NewCustomer input) returns Customer|http:NotFound|error {
        mongodb:UpdateResult result = check customers->updateOne({id}, {set: input});
        if result.matchedCount == 0 {
            return http:NOT_FOUND;
        }
        return {id, ...input};
    }

    resource function delete [string id]() returns http:NoContent|http:NotFound|error {
        mongodb:DeleteResult result = check customers->deleteOne({id});
        if result.deletedCount == 0 {
            return http:NOT_FOUND;
        }
        return http:NO_CONTENT;
    }

    // Historical order data for a customer.
    resource function get [string id]/orders() returns json[]|error {
        stream<record {}, error?> result = check orders->find({customerId: id});
        json[] list = check from record {} o in result select o.toJson();
        return newestFirst(list);
    }
}
