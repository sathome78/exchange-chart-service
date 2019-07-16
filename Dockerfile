FROM java:8
VOLUME /tmp
ARG APP_PATH=/chart
ARG ENVIRONMENT

RUN mkdir -p chart-service
COPY ./target/chart.jar ${APP_PATH}/chart.jar
COPY ./target/config/${ENVIRONMENT}/application.yml ${APP_PATH}/application.yml
COPY ./target/config/${ENVIRONMENT}/log4j2-spring.xml ${APP_PATH}/log4j2-spring.xml

ARG CONFIG_FILE_PATH="-Dspring.config.location="${ENVIRONMENT}"/application.yml"

WORKDIR ${APP_PATH}

EXPOSE 8080
CMD java -jar chart.jar $CONFIG_FILE_PATH