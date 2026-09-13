FROM ballerina/ballerina:2201.12.9
WORKDIR /home/ballerina
COPY Ballerina.toml .
COPY main.bal .
RUN bal build
EXPOSE 9095
CMD ["java", "-jar", "target/bin/delivery_service.jar"]