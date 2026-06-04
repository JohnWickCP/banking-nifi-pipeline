# sync_nifi_scripts.ps1 — copy tất cả .groovy scripts vào NiFi container
# Scripts được copy vào nifi_data volume (/data/scripts/) để tránh virtio-fs bind mount issue trên Windows
# Volume persist qua docker compose stop/start, chỉ mất khi docker compose down -v
# Usage: .\scripts\sync_nifi_scripts.ps1

$CONTAINER = "banking-nifi"
$SCRIPTS_DIR = "$PSScriptRoot\..\nifi\scripts"
$TARGET_DIR  = "/opt/nifi/nifi-current/data/scripts"

Write-Host "[sync-nifi-scripts] Syncing all .groovy scripts to container '$CONTAINER'..."

$files = Get-ChildItem "$SCRIPTS_DIR\*.groovy"
if ($files.Count -eq 0) {
    Write-Host "  No .groovy files found in $SCRIPTS_DIR"
    exit 0
}

foreach ($f in $files) {
    docker cp $f.FullName "${CONTAINER}:${TARGET_DIR}/$($f.Name)"
    Write-Host "  Copied: $($f.Name)"
}

Write-Host ""
Write-Host "[sync-nifi-scripts] Done. Files in container:"
docker exec $CONTAINER ls $TARGET_DIR
