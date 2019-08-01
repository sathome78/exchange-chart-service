FROM java:8
ARG APP_PATH=/opt/chart
ARG ENVIRONMENT
ARG PORT

WORKDIR ${APP_PATH}
COPY ./target/chart.jar chart.jar
VOLUME ${APP_PATH}/indexes
RUN sh -c 'touch chart.jar'

ARG CONFIG_ACTIVE_PROFILE="-Dspring.profiles.active="${ENVIRONMENT}

EXPOSE ${PORT}

CMD java -jar chart.jar ${CONFIG_ACTIVE_PROFILE}