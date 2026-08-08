import ballerina/grpc;
import ballerina/io;

final RentalServiceClient ep = check new ("http://localhost:9090");
