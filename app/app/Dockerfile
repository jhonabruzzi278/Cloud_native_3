FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
# Agente JMX Exporter (Maven Central verifica la suma). Expone MBeans de la JVM en HTTP /metrics para Prometheus.
RUN mvn -q -B dependency:copy -Dartifact=io.prometheus.jmx:jmx_prometheus_javaagent:1.0.1 \
        -DoutputDirectory=/opt/jmx -Dmdep.stripVersion=true
COPY src ./src
RUN mvn -B -ntp verify

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /opt/jmx/jmx_prometheus_javaagent.jar /opt/jmx/jmx_prometheus_javaagent.jar
COPY monitoreo/jmx/config.yml /opt/jmx/config.yml
COPY --from=build /src/target/app.jar /app/app.jar
CMD ["java", "-cp", "/app/app.jar", "cl.duoc.ventas.StreamsApp"]
