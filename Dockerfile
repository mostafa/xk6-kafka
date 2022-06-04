FROM alpine
ARG K6_BINARY

RUN apk add --no-cache ca-certificates && \
    adduser -D -u 12345 -g 12345 k6
COPY ${K6_BINARY} /usr/bin/k6

USER 12345
WORKDIR /home/k6
ENTRYPOINT ["k6"]
