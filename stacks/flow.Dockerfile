# syntax=docker/dockerfile:1.22@sha256:4a43a54dd1fedceb30ba47e76cfcf2b47304f4161c0caeac2db1c61804ea3c91

FROM golang:1.26-alpine@sha256:2389ebfa5b7f43eeafbd6be0c3700cc46690ef842ad962f6c5bd6be49ed82039 AS builder
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
RUN --mount=type=cache,target="/root/.cache/go-build" go build -o /root/peer-flow

FROM alpine:3.23@sha256:25109184c71bdad752c8312a8623239686a9a2071e8825f20acb8f2198c3f659 AS flow-base
ENV TZ=UTC
ADD --checksum=sha256:e5bb2084ccf45087bda1c9bffdea0eb15ee67f0b91646106e466714f9de3c7e3 https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem /usr/local/share/ca-certificates/global-aws-rds-bundle.pem
RUN apk add --no-cache ca-certificates geos && \
  update-ca-certificates && \
  adduser -s /bin/sh -D peerdb
USER peerdb
WORKDIR /home/peerdb
COPY --from=builder --chown=peerdb /root/peer-flow .

FROM flow-base AS flow-api

ARG PEERDB_VERSION_SHA_SHORT
ENV PEERDB_VERSION_SHA_SHORT=${PEERDB_VERSION_SHA_SHORT}

EXPOSE 8112 8113
ENTRYPOINT ["./peer-flow", "api", "--port", "8112", "--gateway-port", "8113"]

FROM flow-base AS flow-worker

# Sane defaults for OpenTelemetry
ENV OTEL_METRIC_EXPORT_INTERVAL=10000
ENV OTEL_EXPORTER_OTLP_COMPRESSION=gzip
ARG PEERDB_VERSION_SHA_SHORT
ENV PEERDB_VERSION_SHA_SHORT=${PEERDB_VERSION_SHA_SHORT}

ENTRYPOINT ["./peer-flow", "worker"]

FROM flow-base AS flow-snapshot-worker

ARG PEERDB_VERSION_SHA_SHORT
ENV PEERDB_VERSION_SHA_SHORT=${PEERDB_VERSION_SHA_SHORT}
ENTRYPOINT ["./peer-flow", "snapshot-worker"]


FROM flow-base AS flow-maintenance

ARG PEERDB_VERSION_SHA_SHORT
ENV PEERDB_VERSION_SHA_SHORT=${PEERDB_VERSION_SHA_SHORT}
ENTRYPOINT ["./peer-flow", "maintenance"]
