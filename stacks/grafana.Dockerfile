# syntax=docker/dockerfile:1.2

FROM grafana/grafana:10.1.5
COPY stacks/grafana/flow_monitoring_dashboard.json /etc/grafana/provisioning/dashboards
COPY stacks/grafana/prometheus_datasource.yml /etc/grafana/provisioning/datasources
COPY stacks/grafana/dashboard.yml /etc/grafana/provisioning/dashboards
