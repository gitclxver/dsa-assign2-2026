import ballerina/http;
import ballerina/time;

listener http:Listener orderListener = new (8083);

//Oder status
type OrderStatus
    "CREATED"
    | "CONFIRMED"
    | "PREPARING"
    | "READY"
    | "OUT_FOR_DELIVERY"
    | "DELIVERED"
    | "CANCELLED";


//oder item
type OrderItem record {|
    string menuItemId;
    string name;
    int quantity;
    decimal price;
|};


// NEW ORDER (request payload)
type NewOrder record {|
    string id;
    string customerId;
    string restaurantId;
    OrderItem[] items;
    decimal totalAmount;
    string deliveryAddress;
|};



// ORDER (stored / response)
type FoodOrder record {|
    string id;
    string customerId;
    string restaurantId;
    OrderItem[] items;
    decimal totalAmount;
    string deliveryAddress;
    OrderStatus status;
    string paymentStatus;
    string? driverId;
    string createdAt;
    string updatedAt;
|};



// TEMPORARY DATABASE
FoodOrder[] orderList = [];



// ORDER SERVICE
service /orders on orderListener {

    // HEALTH CHECK
    // GET /orders/health
    resource function get health() returns json {

        return {
            status: "UP",
            "service": "Order Service",
            port: 8083
        };
    }


  
    // CREATE ORDER
    // POST /orders
    resource function post .(NewOrder req)
        returns FoodOrder|http:BadRequest {

        // Validate customer
        if req.customerId == "" {

            return <http:BadRequest>{
                body: {
                    message: "Customer ID is required"
                }
            };
        }


        // Validate restaurant
        if req.restaurantId == "" {

            return <http:BadRequest>{
                body: {
                    message: "Restaurant ID is required"
                }
            };
        }


        // Validate items
        if req.items.length() == 0 {

            return <http:BadRequest>{
                body: {
                    message: "Order must contain at least one item"
                }
            };
        }


        // Check duplicate order
        foreach FoodOrder existingOrder in orderList {

            if existingOrder.id == req.id {

                return <http:BadRequest>{
                    body: {
                        message: "Order already exists"
                    }
                };
            }
        }


        // Build order with initial state
        string now = getCurrentTime();

        FoodOrder newOrder = {
            id: req.id,
            customerId: req.customerId,
            restaurantId: req.restaurantId,
            items: req.items,
            totalAmount: req.totalAmount,
            deliveryAddress: req.deliveryAddress,
            status: "CREATED",
            paymentStatus: "PENDING",
            driverId: (),
            createdAt: now,
            updatedAt: now
        };


        // Store order
        orderList.push(newOrder);


        return newOrder;
    }


  
    // GET ALL ORDERS
    // GET /orders
    resource function get .() returns FoodOrder[] {

        return orderList;
    }


   
    // GET ORDER BY ID
    // GET /orders/{id}
    resource function get [string id]()
        returns FoodOrder|http:NotFound {

        foreach FoodOrder storedOrder in orderList {

            if storedOrder.id == id {

                return storedOrder;
            }
        }


        return <http:NotFound>{
            body: {
                message: "Order not found"
            }
        };
    }


   
    // CONFIRM ORDER
    // PUT /orders/{id}/confirm
    resource function put [string id]/confirm()
        returns FoodOrder|http:NotFound|http:Conflict {

        return changeOrderStatus(id, "CONFIRMED");
    }


   
    // PREPARING
    // PUT /orders/{id}/preparing
    resource function put [string id]/preparing()
        returns FoodOrder|http:NotFound|http:Conflict {

        return changeOrderStatus(id, "PREPARING");
    }


    // READY
    // PUT /orders/{id}/ready
    resource function put [string id]/ready()
        returns FoodOrder|http:NotFound|http:Conflict {

        return changeOrderStatus(id, "READY");
    }


    // OUT FOR DELIVERY
    // PUT /orders/{id}/outForDelivery
    resource function put [string id]/outForDelivery()
        returns FoodOrder|http:NotFound|http:Conflict {

        return changeOrderStatus(id, "OUT_FOR_DELIVERY");
    }

    
    // DELIVERED
    // PUT /orders/{id}/delivered
    resource function put [string id]/delivered()
        returns FoodOrder|http:NotFound|http:Conflict {

        return changeOrderStatus(id, "DELIVERED");
    }


  
    // CANCEL
    // PUT /orders/{id}/cancel
    resource function put [string id]/cancel()
        returns FoodOrder|http:NotFound|http:Conflict {

        return changeOrderStatus(id, "CANCELLED");
    }
}



// CHANGE ORDER STATUS
function changeOrderStatus(
    string orderId,
    OrderStatus newStatus
)
returns FoodOrder|http:NotFound|http:Conflict {

    int index = -1;


    // Find order
    foreach int i in 0 ..< orderList.length() {

        if orderList[i].id == orderId {

            index = i;

            break;
        }
    }


    // Order not found
    if index == -1 {

        return <http:NotFound>{
            body: {
                message: "Order not found"
            }
        };
    }


    FoodOrder currentOrder = orderList[index];


    // Validate transition
    if !isValidTransition(currentOrder.status, newStatus) {

        return <http:Conflict>{
            body: {
                message:
                    "Invalid order status transition from "
                    + currentOrder.status
                    + " to "
                    + newStatus
            }
        };
    }


    // Change status
    currentOrder.status = newStatus;

    currentOrder.updatedAt = getCurrentTime();


    // Save updated order
    orderList[index] = currentOrder;


    return currentOrder;
}



// VALIDATE ORDER STATUS TRANSITION
function isValidTransition(
    OrderStatus currentStatus,
    OrderStatus nextStatus
)
returns boolean {

    // CREATED → CONFIRMED
    if currentStatus == "CREATED"
        && nextStatus == "CONFIRMED" {

        return true;
    }


    // CREATED → CANCELLED
    if currentStatus == "CREATED"
        && nextStatus == "CANCELLED" {

        return true;
    }


    // CONFIRMED → PREPARING
    if currentStatus == "CONFIRMED"
        && nextStatus == "PREPARING" {

        return true;
    }


    // CONFIRMED → CANCELLED
    if currentStatus == "CONFIRMED"
        && nextStatus == "CANCELLED" {

        return true;
    }


    // PREPARING → READY
    if currentStatus == "PREPARING"
        && nextStatus == "READY" {

        return true;
    }


    // READY → OUT_FOR_DELIVERY
    if currentStatus == "READY"
        && nextStatus == "OUT_FOR_DELIVERY" {

        return true;
    }


    // OUT_FOR_DELIVERY → DELIVERED
    if currentStatus == "OUT_FOR_DELIVERY"
        && nextStatus == "DELIVERED" {

        return true;
    }


    return false;
}



// GET CURRENT TIME
function getCurrentTime() returns string {

    return time:utcToString(time:utcNow());
}
