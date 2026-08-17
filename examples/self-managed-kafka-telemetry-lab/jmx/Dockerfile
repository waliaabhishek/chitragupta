FROM eclipse-temurin:21-jre

ARG JMX_EXPORTER_VERSION=1.5.0
ADD https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_VERSION}/jmx_prometheus_standalone-${JMX_EXPORTER_VERSION}.jar /opt/jmx/jmx_prometheus_standalone.jar
RUN chmod 0444 /opt/jmx/jmx_prometheus_standalone.jar

USER 65534:65534
ENTRYPOINT ["java", "-jar", "/opt/jmx/jmx_prometheus_standalone.jar", "7071"]

