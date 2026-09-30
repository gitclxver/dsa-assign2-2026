import ballerina/http;
import ballerina/log;
import ballerina/uuid;

configurable int port = 9091;

@http:ServiceConfig {
    // Lets the HTML dashboard (opened from disk / another port) call this API.
    cors: {allowOrigins: ["*"], allowMethods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"]}
}
isolated service / on new http:Listener(port) {

    function init() {
        log:printInfo("Customer Service started", port = port);
    }

    resource function get health() returns map<string> => {status: "UP", 'service: "customer-service"};

    // ------------------------------------------------------------------ accounts

    resource function get customers(string? email) returns Customer[]|error {
        if email is string {
            Customer? customer = check findCustomerByEmail(email);
            if customer is Customer {
                return [customer];
            }
            return [];
        }
        return findCustomers();
    }

    resource function post customers(CustomerInput input) returns http:Created|http:Conflict|error {
        if check findCustomerByEmail(input.email) is Customer {
            return <http:Conflict>{body: <ErrorBody>{message: string `Email '${input.email}' is already registered`}};
        }
        string timestamp = now();
        Customer customer = {
            id: uuid:createType4AsString(),
            name: input.name,
            email: input.email,
            phone: input.phone,
            addresses: normaliseDefault(from AddressInput a in input.addresses select toAddress(a)),
            createdAt: timestamp,
            updatedAt: timestamp
        };
        error? result = insertCustomer(customer);
        if result is error {
            return isDuplicateKey(result)
                ? <http:Conflict>{body: <ErrorBody>{message: string `Email '${input.email}' is already registered`}}
                : result;
        }
        log:printInfo("Customer registered", customerId = customer.id);
        return <http:Created>{headers: {"Location": string `/customers/${customer.id}`}, body: customer};
    }

    resource function get customers/[string id]() returns Customer|http:NotFound|error {
        Customer? customer = check findCustomer(id);
        return customer ?: customerNotFound(id);
    }

    resource function put customers/[string id](CustomerUpdate changes) returns Customer|http:NotFound|http:Conflict|error {
        string? email = changes.email;
        if email is string {
            Customer? owner = check findCustomerByEmail(email);
            if owner is Customer && owner.id != id {
                return <http:Conflict>{body: <ErrorBody>{message: string `Email '${email}' is already registered`}};
            }
        }
        map<json> fields = check changes.toJson().ensureType();
        fields["updatedAt"] = now();
        if !check setCustomerFields(id, fields) {
            return customerNotFound(id);
        }
        return <Customer>check findCustomer(id);
    }

    resource function delete customers/[string id]() returns http:NoContent|http:NotFound|error {
        return check deleteCustomer(id) ? http:NO_CONTENT : customerNotFound(id);
    }

    // ----------------------------------------------------------------- addresses

    resource function get customers/[string id]/addresses() returns Address[]|http:NotFound|error {
        Customer? customer = check findCustomer(id);
        return customer is Customer ? customer.addresses : customerNotFound(id);
    }

    resource function post customers/[string id]/addresses(AddressInput input) returns http:Created|http:NotFound|error {
        Customer? customer = check findCustomer(id);
        if customer is () {
            return customerNotFound(id);
        }
        Address address = toAddress(input);
        Address[] addresses = [...customer.addresses, address];
        _ = check saveAddresses(id, address.isDefault ? makeDefault(addresses, address.id) : normaliseDefault(addresses));
        return <http:Created>{
            headers: {"Location": string `/customers/${id}/addresses/${address.id}`},
            body: check findAddress(id, address.id)
        };
    }

    resource function put customers/[string id]/addresses/[string addressId](AddressUpdate changes)
            returns Address|http:NotFound|error {
        Customer? customer = check findCustomer(id);
        if customer is () {
            return customerNotFound(id);
        }
        Address[] addresses = customer.addresses;
        int? index = ();
        foreach int i in 0 ..< addresses.length() {
            if addresses[i].id == addressId {
                index = i;
            }
        }
        if index is () {
            return addressNotFound(addressId);
        }
        addresses[index] = applyAddressUpdate(addresses[index], changes);
        if changes.isDefault == true {
            addresses = makeDefault(addresses, addressId);
        }
        _ = check saveAddresses(id, normaliseDefault(addresses));
        return check findAddress(id, addressId);
    }

    resource function delete customers/[string id]/addresses/[string addressId]() returns http:NoContent|http:NotFound|error {
        Customer? customer = check findCustomer(id);
        if customer is () {
            return customerNotFound(id);
        }
        Address[] remaining = from Address a in customer.addresses where a.id != addressId select a;
        if remaining.length() == customer.addresses.length() {
            return addressNotFound(addressId);
        }
        _ = check saveAddresses(id, normaliseDefault(remaining));
        return http:NO_CONTENT;
    }

    // ------------------------------------------------------------- order history

    resource function get customers/[string id]/orders(string? status, int 'limit = 50)
            returns OrderHistory[]|http:NotFound|error {
        if check findCustomer(id) is () {
            return customerNotFound(id);
        }
        return findOrdersForCustomer(id, status, 'limit);
    }

    // Wrapped in http:Ok: returning the bare OrderHistory record (which has a `status` field) alongside
    // http:NotFound makes the 2201.12 compiler hang while checking the status-code response union.
    resource function get customers/[string id]/orders/[string orderId]() returns http:Ok|http:NotFound|error {
        OrderHistory? found = check findOrder(id, orderId);
        if found is OrderHistory {
            return <http:Ok>{body: found};
        }
        return orderNotFound(id, orderId);
    }
}

// ------------------------------------------------------------------- helpers

isolated function toAddress(AddressInput input) returns Address {
    Address address = {
        id: uuid:createType4AsString(),
        label: input.label,
        street: input.street,
        city: input.city,
        isDefault: input.isDefault
    };
    if input.postalCode is string {
        address.postalCode = input.postalCode;
    }
    if input.instructions is string {
        address.instructions = input.instructions;
    }
    if input.latitude is float {
        address.latitude = input.latitude;
    }
    if input.longitude is float {
        address.longitude = input.longitude;
    }
    return address;
}

isolated function applyAddressUpdate(Address current, AddressUpdate changes) returns Address {
    Address updated = current.clone();
    updated.label = changes.label ?: current.label;
    updated.street = changes.street ?: current.street;
    updated.city = changes.city ?: current.city;
    updated.isDefault = changes.isDefault ?: current.isDefault;
    if changes.postalCode is string {
        updated.postalCode = changes.postalCode;
    }
    if changes.instructions is string {
        updated.instructions = changes.instructions;
    }
    if changes.latitude is float {
        updated.latitude = changes.latitude;
    }
    if changes.longitude is float {
        updated.longitude = changes.longitude;
    }
    return updated;
}

// Marks exactly one address as default.
isolated function makeDefault(Address[] addresses, string defaultId) returns Address[] {
    Address[] result = [];
    foreach Address a in addresses {
        Address copy = a.clone();
        copy.isDefault = a.id == defaultId;
        result.push(copy);
    }
    return result;
}

// Guarantees a single default address: keeps the first flagged one, or promotes the first address.
isolated function normaliseDefault(Address[] addresses) returns Address[] {
    if addresses.length() == 0 {
        return addresses;
    }
    Address[] flagged = from Address a in addresses where a.isDefault select a;
    return makeDefault(addresses, flagged.length() > 0 ? flagged[0].id : addresses[0].id);
}

isolated function saveAddresses(string customerId, Address[] addresses) returns boolean|error =>
    setCustomerFields(customerId, {addresses: addresses.toJson(), updatedAt: now()});

isolated function findAddress(string customerId, string addressId) returns Address|error {
    Customer? customer = check findCustomer(customerId);
    Address[] addresses = customer is Customer ? customer.addresses : [];
    Address[] matches = from Address a in addresses where a.id == addressId select a;
    return matches.length() > 0 ? matches[0] : error(string `Address '${addressId}' not found`);
}

isolated function isDuplicateKey(error err) returns boolean => err.message().includes("E11000");

isolated function customerNotFound(string id) returns http:NotFound =>
    {body: <ErrorBody>{message: string `Customer '${id}' not found`}};

isolated function addressNotFound(string id) returns http:NotFound =>
    {body: <ErrorBody>{message: string `Address '${id}' not found`}};

isolated function orderNotFound(string customerId, string orderId) returns http:NotFound =>
    {body: <ErrorBody>{message: string `Order '${orderId}' not found for customer '${customerId}'`}};
