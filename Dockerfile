# syntax=docker/dockerfile:1
# Multi-stage build: compile and package with the Maven Wrapper on a JDK image, run on a JRE image.

FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY mvnw pom.xml ./
COPY .mvn .mvn
RUN chmod +x mvnw
# Resolve dependencies in their own layer so source changes do not re-download them.
RUN --mount=type=cache,target=/root/.m2 ./mvnw -B -q dependency:go-offline
COPY src src
RUN --mount=type=cache,target=/root/.m2 ./mvnw -B -q package -DskipTests \
    && cp target/distributed-task-platform-*.jar /src/app.jar

FROM eclipse-temurin:21-jre
LABEL org.opencontainers.image.title="distributed-task-platform" \
      org.opencontainers.image.licenses="MIT" \
      cvproject="distributed-task-platform"
RUN groupadd --system app && useradd --system --gid app --home /app app
WORKDIR /app
COPY --from=build /src/app.jar /app/app.jar
USER app
ENV BIND_ADDRESS=0.0.0.0 \
    PORT=18080 \
    JAVA_OPTS="-XX:MaxRAMPercentage=75"
EXPOSE 18080
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
