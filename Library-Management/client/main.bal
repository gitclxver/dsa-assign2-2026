import ballerina/http;
import ballerina/io;
import ballerina/uuid;

final http:Client apiFetch = check new ("http://localhost:8081/api");

const string ADMIN_PASSWORD = "admin123";

// Data models

public type Componet record {
    string compIf;
    string name;
    string description;
};

public type Schedule record {
    string scheduleId;
    string 'type;
    string dueDate;
    string description;
};

public type Task record {
    string taskId;
    string description;
};

public type WorkOrder record {
    string orderId;
    string status;
    string description;
    Task[] task?;
};