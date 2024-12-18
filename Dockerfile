FROM alpine:3.20

ARG VERSION_TAG
ARG TARGETOS
ARG TARGETARCH

RUN apk add --no-cache ca-certificates=20240705-r0 openssl=3.3.2-r1 && \
    adduser -D -u 12345 -g 12345 k6
COPY ./dist/xk6-kafka_${VERSION_TAG}_${TARGETOS}_${TARGETARCH} /usr/bin/k6

USER 12345
WORKDIR /home/k6
ENTRYPOINT ["k6"]
