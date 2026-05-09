FROM maven:3.9-eclipse-temurin-11 as builder

WORKDIR /build
COPY . .
RUN mvn clean package -DskipTests

FROM eclipse-temurin:11-jre-jammy

WORKDIR /app
COPY --from=builder /build/target/crypto-quote-producer.jar .

# Verify the JAR was created
RUN ls -lh crypto-quote-producer.jar

ENTRYPOINT ["java", "-jar", "crypto-quote-producer.jar"]
