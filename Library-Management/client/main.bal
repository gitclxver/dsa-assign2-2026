import ballerina/http;
import ballerina/io;
import ballerina/uuid;

final http:Client apiFetch = check new ("http://localhost:8081/api");