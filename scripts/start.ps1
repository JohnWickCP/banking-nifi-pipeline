# start.ps1 — Khởi động toàn bộ stack và tự động setup Grafana
# Chạy từ thư mục gốc project: .\scripts\start.ps1

Set-Location "$PSScriptRoot\.."

Write-Host "==> Starting Docker stack..." -ForegroundColor Cyan
docker compose -f docker/docker-compose.yml up -d

Write-Host "==> Waiting for Grafana to be healthy (30s)..." -ForegroundColor Cyan
$timeout = 60
$elapsed = 0
do {
    Start-Sleep -Seconds 5
    $elapsed += 5
    $status = docker inspect --format='{{.State.Health.Status}}' banking-grafana 2>$null
    Write-Host "  [$elapsed s] Grafana status: $status"
} while ($status -ne "healthy" -and $elapsed -lt $timeout)

Write-Host "==> Copying Grafana provisioning files (Windows bind-mount workaround)..." -ForegroundColor Cyan
docker exec -u root banking-grafana mkdir -p /var/lib/grafana/dashboards
docker cp "grafana/provisioning/datasources/postgres.yaml"  "banking-grafana://etc/grafana/provisioning/datasources/postgres.yaml"
docker cp "grafana/provisioning/dashboards/banking.yaml"    "banking-grafana://etc/grafana/provisioning/dashboards/banking.yaml"
docker cp "grafana/dashboards/banking_overview.json"        "banking-grafana://var/lib/grafana/dashboards/banking_overview.json"

Write-Host "==> Disabling Grafana brute-force protection..." -ForegroundColor Cyan
docker exec -u root banking-grafana sh -c "printf '\n[security]\ndisable_brute_force_login_protection = true\n' >> /etc/grafana/grafana.ini"

Write-Host "==> Restarting Grafana to apply config..." -ForegroundColor Cyan
docker restart banking-grafana
Start-Sleep -Seconds 8

Write-Host ""
Write-Host "==> Stack ready!" -ForegroundColor Green
Write-Host "  NiFi    : https://localhost:8443/nifi  (admin / Banking@Admin1)"
Write-Host "  Grafana : http://localhost:3000        (admin / admin123)"
Write-Host "  MinIO   : http://localhost:9001        (minioadmin / minioadmin123)"
Write-Host "  Postgres: localhost:5432               (banking / banking123)"
