# syntax=docker/dockerfile:1.2

FROM grafana/grafana:latest
COPY stacks/grafana/flow_monitoring_dashboard.json stacks/grafana/dashboard.yml /etc/grafana/provisioning/dashboards
COPY stacks/grafana/prometheus_datasource.yml /etc/grafana/provisioning/datasources
