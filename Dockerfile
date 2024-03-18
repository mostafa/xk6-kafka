FROM alpine

ARG VERSION_TAG
ARG TARGETOS
ARG TARGETARCH

RUN apk add --no-cache ca-certificates && \
    adduser -D -u 12345 -g 12345 k6
COPY ./dist/xk6-kafka_${VERSION_TAG}_${TARGETOS}_${TARGETARCH} /usr/bin/k6

USER 12345
WORKDIR /home/k6
ENTRYPOINT ["k6"]
