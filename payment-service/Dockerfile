FROM ballerina/ballerina:2201.12.9
WORKDIR /home/ballerina
COPY Ballerina.toml .
COPY main.bal .
RUN bal build
CMD ["java", "-jar", "target/bin/payment_service.jar"]