# syntax=docker/dockerfile:1.23@sha256:2780b5c3bab67f1f76c781860de469442999ed1a0d7992a5efdf2cffc0e3d769

FROM golang:1.26-alpine@sha256:91eda9776261207ea25fd06b5b7fed8d397dd2c0a283e77f2ab6e91bfa71079d AS builder
# Allow build flags to be passed in at build time, for example debug flags
ARG DEBUG_BUILD
ENV DEBUG_BUILD=${DEBUG_BUILD}

RUN apk add --no-cache gcc geos-dev musl-dev
WORKDIR /root/flow

# first copy only go.mod and go.sum to cache dependencies
COPY flow/go.mod flow/go.sum ./
COPY flow/pkg/go.mod flow/pkg/go.sum ./pkg/

# download all the dependencies
RUN go mod download

# Copy all the code
COPY flow .
RUN rm -f go.work*

# build the binary from flow folder
WORKDIR /root/flow
ENV CGO_ENABLED=1
# Generate the typed handler wrapper
RUN go generate
ENV GOCACHE=/root/.cache/go-build
RUN --mount=type=cache,target="/root/.cache/go-build" go build ${DEBUG_BUILD:+-gcflags} ${DEBUG_BUILD:+"all=-N -l"} -o /root/peer-flow
RUN --mount=type=cache,target="/root/.cache/go-build" if [[ "$DEBUG_BUILD" = "1" ]]; then \
    go install github.com/go-delve/delve/cmd/dlv@latest; \
  fi

FROM alpine:3.23@sha256:5b10f432ef3da1b8d4c7eb6c487f2f5a8f096bc91145e68878dd4a5019afde11 AS flow-base
ENV TZ=UTC
ADD --checksum=sha256:e5bb2084ccf45087bda1c9bffdea0eb15ee67f0b91646106e466714f9de3c7e3 https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem /usr/local/share/ca-certificates/global-aws-rds-bundle.pem
RUN apk add --no-cache ca-certificates geos && \
  update-ca-certificates && \
  adduser -s /bin/sh -D peerdb
USER peerdb
WORKDIR /home/peerdb
COPY --from=builder --chown=peerdb /root/peer-flow .
ENTRYPOINT [ "/home/peerdb/peer-flow" ]

# Debug Image with Delve installed and the binary built with debug flags
FROM flow-base AS flow-base-debug
USER root
COPY --from=builder /go/bin/dlv /usr/local/bin/dlv
ENV TEMPORAL_DEBUG=1
EXPOSE 40000
ENTRYPOINT ["dlv", "--headless", "--continue", "--accept-multiclient", "--listen=:40000", "--api-version=2", "exec", "/home/peerdb/peer-flow", "--"]

FROM flow-base AS flow-api

ARG PEERDB_VERSION_SHA_SHORT
ENV PEERDB_VERSION_SHA_SHORT=${PEERDB_VERSION_SHA_SHORT}

EXPOSE 8112 8113
ENTRYPOINT [ "/home/peerdb/peer-flow", "api", "--port", "8112", "--gateway-port", "8113"]

FROM flow-base-debug AS flow-api-debug

ARG PEERDB_VERSION_SHA_SHORT
ENV PEERDB_VERSION_SHA_SHORT=${PEERDB_VERSION_SHA_SHORT}

EXPOSE 8112 8113
CMD ["api", "--port", "8112", "--gateway-port", "8113"]

FROM flow-base AS flow-worker

USER root
RUN apk add --no-cache postgresql-client
USER peerdb

# Sane defaults for OpenTelemetry
ENV OTEL_METRIC_EXPORT_INTERVAL=10000
ENV OTEL_EXPORTER_OTLP_COMPRESSION=gzip
ARG PEERDB_VERSION_SHA_SHORT
ENV PEERDB_VERSION_SHA_SHORT=${PEERDB_VERSION_SHA_SHORT}

ENTRYPOINT [ "/home/peerdb/peer-flow", "worker"]

FROM flow-base-debug AS flow-worker-debug
ENV OTEL_METRIC_EXPORT_INTERVAL=10000
ENV OTEL_EXPORTER_OTLP_COMPRESSION=gzip
ARG PEERDB_VERSION_SHA_SHORT
ENV PEERDB_VERSION_SHA_SHORT=${PEERDB_VERSION_SHA_SHORT}
CMD ["worker"]


FROM flow-base AS flow-snapshot-worker

ARG PEERDB_VERSION_SHA_SHORT
ENV PEERDB_VERSION_SHA_SHORT=${PEERDB_VERSION_SHA_SHORT}
ENTRYPOINT [ "/home/peerdb/peer-flow", "snapshot-worker"]

FROM flow-base-debug AS flow-snapshot-worker-debug

ARG PEERDB_VERSION_SHA_SHORT
ENV PEERDB_VERSION_SHA_SHORT=${PEERDB_VERSION_SHA_SHORT}
CMD ["snapshot-worker"]


FROM flow-base AS flow-maintenance

ARG PEERDB_VERSION_SHA_SHORT
ENV PEERDB_VERSION_SHA_SHORT=${PEERDB_VERSION_SHA_SHORT}
ENTRYPOINT [ "/home/peerdb/peer-flow", "maintenance"]
