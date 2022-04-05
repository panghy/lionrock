FROM adoptopenjdk/openjdk11:latest

COPY release/start.bash /bash/start.bash
RUN chmod a+x /bash/start.bash

WORKDIR /

ARG JAR_FILE=build/libs/lionrock-inmemory-server-*.*.*-boot.jar
COPY ${JAR_FILE} app.jar

EXPOSE 6565

CMD /bash/start.bash