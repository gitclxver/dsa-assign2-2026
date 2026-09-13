This project is an introductory Distributed Systems Demonstration built with:

Ballerina
Apache Kafka
MongoDB
Docker

It demonstrates:

1. Microservices Architecture: Independent Services with clear functional boundaries
2. Event Driven Coordination: Async Order processing using Kafka pub/sun topics
3. Persistant State: Relational and Document state stored in MongoDB
4. Extra* Dynamic Surge Pricing: Real-Time pricing adjustment calculated based on active platform demand


Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)

Steps to run project.

Step 1:
    docker compose up -d
    docker compose ps

Step 2:
    seed data (Yet to be implemented)

Step 3: 
    Option 1:
    Double Click client/html/index.html to view dashboard 
    Option 2: 
    Use client/cli.bal


When you're done testing 

Run:
    docker compose down -v


