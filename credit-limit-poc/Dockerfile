# One image recipe for every service. Build the jars first with: mvn clean package -DskipTests
FROM eclipse-temurin:21-jre

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

ARG MODULE
WORKDIR /app
COPY ${MODULE}/target/${MODULE}.jar /app/app.jar

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar \"$@\"", "--"]
