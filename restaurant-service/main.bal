import ballerina/http;

// Restaurant records
type Restaurant record {|
    int id;
    string name;
    string address;
    boolean kitchenOpen;
|};

// Menu item
type MenuItem record {|
    int id;
    int restaurantId;
    string name;
    string description;
    decimal price;
    int stock;
    boolean available;
|};

// I put random data for now
Restaurant[] restaurants = [
    {
        id: 1,
        name: "Kasi Burger",
        address: "Windhoek",
        kitchenOpen: true
    }
];

MenuItem[] menuItems = [
    {
        id: 1,
        restaurantId: 1,
        name: "kasi Burger",
        description: "kasi burger with salsa",
        price: 14.00,
        stock: 20,
        available: true
    }
];

service /restaurant on new http:Listener(9090) {

    // Get restaurant details
    resource function get [int restaurantId]() returns Restaurant|http:NotFound {
        foreach Restaurant restaurant in restaurants {
            if restaurant.id == restaurantId {
                return restaurant;
            }
        }

        return http:NOT_FOUND;
    }

    // Get restaurant menu
    resource function get [int restaurantId]/menu() returns MenuItem[] {
        MenuItem[] result = [];

        foreach MenuItem item in menuItems {
            if item.restaurantId == restaurantId {
                result.push(item);
            }
        }

        return result;
    }

    // Add menu item
    resource function post [int restaurantId]/menu(MenuItem item)
            returns MenuItem|http:BadRequest {

        item.restaurantId = restaurantId;
        menuItems.push(item);

        return item;
    }

    // Update menu item
    resource function put menu/[int itemId](MenuItem updatedItem)
            returns MenuItem|http:NotFound {

        foreach int index in 0 ..< menuItems.length() {
            if menuItems[index].id == itemId {
                updatedItem.id = itemId;
                menuItems[index] = updatedItem;

                return updatedItem;
            }
        }

        return http:NOT_FOUND;
    }

    // Delete menu item
    resource function delete menu/[int itemId]()
            returns string|http:NotFound {

        foreach int index in 0 ..< menuItems.length() {
            if menuItems[index].id == itemId {
                _ = menuItems.remove(index);
                return "Menu item deleted";
            }
        }

        return http:NOT_FOUND;
    }

    // Update stock
    resource function put menu/[int itemId]/inventory(int stock)
            returns MenuItem|http:NotFound {

        foreach int index in 0 ..< menuItems.length() {
            if menuItems[index].id == itemId {

                menuItems[index].stock = stock;

                if stock > 0 {
                    menuItems[index].available = true;
                } else {
                    menuItems[index].available = false;
                }

                return menuItems[index];
            }
        }

        return http:NOT_FOUND;
    }

    // Open or close kitchen
    resource function put [int restaurantId]/kitchen(boolean open)
            returns Restaurant|http:NotFound {

        foreach int index in 0 ..< restaurants.length() {
            if restaurants[index].id == restaurantId {

                restaurants[index].kitchenOpen = open;

                return restaurants[index];
            }
        }

        return http:NOT_FOUND;
    }
}