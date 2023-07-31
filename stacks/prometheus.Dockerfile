# syntax=docker/dockerfile:1.2

FROM prom/prometheus:latest
COPY stacks/prometheus/prometheus.yml /etc/prometheus