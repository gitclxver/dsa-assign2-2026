FROM ballerina/ballerina:2201.12.9
WORKDIR /home/ballerina
COPY Ballerina.toml .
COPY main.bal .
RUN bal build
EXPOSE 9093
CMD ["java", "-jar", "target/bin/order_service.jar"]