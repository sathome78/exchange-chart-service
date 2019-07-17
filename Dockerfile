FROM java:8
ARG APP_PATH=/opt/chart
ARG ENVIRONMENT

WORKDIR ${APP_PATH}
COPY ./target/chart.jar chart.jar
VOLUME ${APP_PATH}/indexes
RUN sh -c 'touch chart.jar'
ENTRYPOINT ["java","-jar","-Dspring.profiles.active=${ENVIRONMENT}","chart.jar"]