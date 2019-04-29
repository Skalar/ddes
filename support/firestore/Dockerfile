FROM google/cloud-sdk:243.0.0-alpine

RUN apk add --update --no-cache openjdk8-jre &&\
    gcloud components install cloud-firestore-emulator beta --quiet

VOLUME /opt/data

COPY start-firestore .

EXPOSE 8082

ENTRYPOINT ["./start-firestore"]