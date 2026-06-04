FROM grafana/grafana:10.0.0

USER root

# Bake provisioning into image — avoids Windows Docker Desktop bind-mount issues
COPY grafana/provisioning/datasources/ /etc/grafana/provisioning/datasources/
COPY grafana/provisioning/dashboards/  /etc/grafana/provisioning/dashboards/

# Dashboard JSON goes to /usr/share/grafana/dashboards — NOT overridden by grafana_data volume
RUN mkdir -p /usr/share/grafana/dashboards
COPY grafana/dashboards/ /usr/share/grafana/dashboards/

# Disable brute-force login protection permanently
RUN printf '\n[security]\ndisable_brute_force_login_protection = true\n' >> /etc/grafana/grafana.ini

RUN chown -R 472:472 \
    /etc/grafana/provisioning \
    /usr/share/grafana/dashboards

USER 472
