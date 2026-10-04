import ballerina/http;
import ballerina/log;
import ballerina/os;
import ballerinax/mongodb;

// Restaurant information
type Restaurant record {|
    string id;
    string name;
    string address;
    boolean kitchenOpen;
|};

// Menu item information
type MenuItem record {|
    string id;
    string restaurantId;
    string name;
    string description;
    decimal price;
    int stock;
    boolean available;
|};

const string DB_NAME = "restaurant_db";

string mongoUrl = envOr("MONGO_URL", "mongodb://mongodb:27017");

mongodb:Client mongoClient = check new({
    connection: mongoUrl
});

mongodb:Collection restaurants = openCollection("restaurants");
mongodb:Collection menuItems = openCollection("menuItems");


// Get a value from the environment, or use a default value
function envOr(string name, string defaultValue) returns string {
    string|error value = os:getEnv(name);

    if value is string && value != "" {
        return value;
    }

    return defaultValue;
}


// Connect to a MongoDB collection
function openCollection(string collectionName) returns mongodb:Collection {
    return mongoClient.getDatabase(DB_NAME).getCollection(collectionName);
}


// Create the indexes and add sample data when the service starts
function init() returns error? {
    check restaurants.createIndex({
        key: { id: 1 },
        name: "restaurant_id_unique",
        unique: true
    });

    check menuItems.createIndex({
        key: { id: 1 },
        name: "menu_item_id_unique",
        unique: true
    });

    check addSampleData();

    log:printInfo("Restaurant service started");
}


// Add sample restaurant and menu data if the database is empty
function addSampleData() returns error? {
    mongodb:Record{}|error restaurantCount = check restaurants.countDocuments({});

    if restaurantCount is int && restaurantCount == 0 {
        Restaurant restaurant = {
            id: "rest-001",
            name: "Kasi Burger",
            address: "Windhoek",
            kitchenOpen: true
        };

        check restaurants.insertOne(restaurant);
    }

    mongodb:Record{}|error menuCount = check menuItems.countDocuments({});

    if menuCount is int && menuCount == 0 {
        MenuItem item = {
            id: "item-001",
            restaurantId: "rest-001",
            name: "Kasi Burger",
            description: "Burger with chips",
            price: 14.00,
            stock: 20,
            available: true
        };

        check menuItems.insertOne(item);
    }
}


// Find a restaurant using its ID
function findRestaurant(string restaurantId) returns Restaurant|error? {
    mongodb:Document|error result = restaurants.findOne({
        id: restaurantId
    });

    if result is mongodb:Document {
        return result.cloneWithType(Restaurant);
    }

    return result;
}


// Find a menu item using its ID
function findMenuItem(string itemId) returns MenuItem|error? {
    mongodb:Document|error result = menuItems.findOne({
        id: itemId
    });

    if result is mongodb:Document {
        return result.cloneWithType(MenuItem);
    }

    return result;
}


// Check if the restaurant service is running
resource function get health() returns string {
    return "Restaurant service is running";
}


// Get restaurant details
resource function get restaurants/[string restaurantId]() returns Restaurant|http:NotFound {
    Restaurant|error? restaurant = findRestaurant(restaurantId);

    if restaurant is Restaurant {
        return restaurant;
    }

    return http:NOT_FOUND;
}


// Open or close the restaurant kitchen
resource function put restaurants/[string restaurantId]/kitchen(
    boolean kitchenOpen
) returns Restaurant|http:NotFound {
    Restaurant|error? restaurant = findRestaurant(restaurantId);

    if restaurant is () || restaurant is error {
        return http:NOT_FOUND;
    }

    mongodb:UpdateResult|error result = restaurants.updateOne(
        {id: restaurantId},
        {set: {kitchenOpen: kitchenOpen}}
    );

    if result is error {
        return http:NOT_FOUND;
    }

    restaurant.kitchenOpen = kitchenOpen;
    return restaurant;
}


// Get all menu items for a restaurant
resource function get restaurants/[string restaurantId]/menu()
    returns MenuItem[]|http:NotFound {

    Restaurant|error? restaurant = findRestaurant(restaurantId);

    if restaurant is () || restaurant is error {
        return http:NOT_FOUND;
    }

    mongodb:Document[]|error result = menuItems.findMany({
        restaurantId: restaurantId
    });

    if result is error {
        return http:NOT_FOUND;
    }

    MenuItem[] items = [];

    foreach mongodb:Document document in result {
        MenuItem|error item = document.cloneWithType(MenuItem);

        if item is MenuItem {
            items.push(item);
        }
    }

    return items;
}


// Add a new item to a restaurant menu
resource function post restaurants/[string restaurantId]/menu(
    MenuItem item
) returns MenuItem|http:BadRequest|http:NotFound {

    if item.id == "" || item.name == "" {
        return http:BAD_REQUEST;
    }

    if item.price < 0 || item.stock < 0 {
        return http:BAD_REQUEST;
    }

    Restaurant|error? restaurant = findRestaurant(restaurantId);

    if restaurant is () || restaurant is error {
        return http:NOT_FOUND;
    }

    item.restaurantId = restaurantId;

    MenuItem|error existingItem = findMenuItem(item.id);

    if existingItem is MenuItem {
        return http:BAD_REQUEST;
    }

    mongodb:InsertResult|error result = menuItems.insertOne(item);

    if result is error {
        return http:BAD_REQUEST;
    }

    return item;
}


// Update an existing menu item
resource function put menu/[string itemId](
    MenuItem item
) returns MenuItem|http:BadRequest|http:NotFound {

    if item.name == "" || item.price < 0 || item.stock < 0 {
        return http:BAD_REQUEST;
    }

    MenuItem|error? existingItem = findMenuItem(itemId);

    if existingItem is () || existingItem is error {
        return http:NOT_FOUND;
    }

    mongodb:UpdateResult|error result = menuItems.updateOne(
        {id: itemId},
        {
            set: {
                name: item.name,
                description: item.description,
                price: item.price,
                stock: item.stock,
                available: item.available
            }
        }
    );

    if result is error {
        return http:BAD_REQUEST;
    }

    item.id = itemId;
    item.restaurantId = existingItem.restaurantId;

    return item;
}


// Delete a menu item
resource function delete menu/[string itemId]() returns string|http:NotFound {
    mongodb:DeleteResult|error result = menuItems.deleteOne({
        id: itemId
    });

    if result is error {
        return http:NOT_FOUND;
    }

    return "Menu item deleted";
}


// Change the stock level of a menu item
resource function put menu/[string itemId]/inventory(
    int stock
) returns MenuItem|http:BadRequest|http:NotFound {

    if stock < 0 {
        return http:BAD_REQUEST;
    }

    MenuItem|error? existingItem = findMenuItem(itemId);

    if existingItem is () || existingItem is error {
        return http:NOT_FOUND;
    }

    boolean available = stock > 0;

    mongodb:UpdateResult|error result = menuItems.updateOne(
        {id: itemId},
        {
            set: {
                stock: stock,
                available: available
            }
        }
    );

    if result is error {
        return http:BAD_REQUEST;
    }

    existingItem.stock = stock;
    existingItem.available = available;

    return existingItem;
}


// Start the REST API on port 9092
service /restaurant on new http:Listener(9092) {

}