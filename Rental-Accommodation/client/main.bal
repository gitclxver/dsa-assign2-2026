import ballerina/grpc;
import ballerina/io;

final RentalServiceClient ep = check new ("http://localhost:9090");

const string ADMIN_PASSWORD = "admin123";


public function main() returns error?{
    io:println("Rental Accommodation gRPC Client");

    //Auto seed demo properties before the menus.
    io:println("\nSeeding Demo Data");
    error? seedResults = seedDemo();

    //Will add later today
}
