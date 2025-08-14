<# 
Edge Device stack manager (Windows PowerShell)
File: edgectl.ps1
Usage examples:
  .\edgectl.ps1 up                 # start whole stack
  .\edgectl.ps1 up --build         # rebuild + start
  .\edgectl.ps1 down               # stop stack (keeps volumes)
  .\edgectl.ps1 restart            # restart all services
  .\edgectl.ps1 restart -s kafka   # restart only one service
  .\edgectl.ps1 status             # show container status + health
  .\edgectl.ps1 logs -s dashboard-api -f -n 300
  .\edgectl.ps1 exec -s postgres -c "psql -U edge -d edgedb"
  .\edgectl.ps1 seed-db            # re-run init.sql and optional seed_data.sql
  .\edgectl.ps1 backup             # backup Postgres (and Influx if available)
  .\edgectl.ps1 enable-autostart   # start stack automatically on boot
  .\edgectl.ps1 disable-autostart  # remove the autostart task
#>

[CmdletBinding()]
param(
  [Parameter(Position=0)]
  [ValidateSet('up','down','restart','status','logs','exec','seed-db','backup','pull','build','rebuild','prune','enable-autostart','disable-autostart')]
  [string]$Command = 'status',

  # optional: target service for logs/exec/restart
  [string]$s,

  # optional: command to run inside a container (for 'exec')
  [string]$c,

  # optional: follow logs (-f) and number of lines (-n)
  [switch]$f,
  [int]$n = 200
)

# --- Helpers ------------------------------------------------------------------

$ErrorActionPreference = 'Stop'

function Write-Info($msg){ Write-Host "[INFO] $msg" -ForegroundColor Cyan }
function Write-Ok($msg){ Write-Host "[ OK ] $msg" -ForegroundColor Green }
function Write-Warn($msg){ Write-Warning "$msg" }
function Write-Err($msg){ Write-Host "[ERR] $msg" -ForegroundColor Red }

# Ensure we run from the repo root (same folder as docker-compose.yml)
$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptRoot

$composeFile = Join-Path $scriptRoot 'docker-compose.yml'
if(-not (Test-Path $composeFile)){
  Write-Err "docker-compose.yml not found next to edgectl.ps1. Place the script in your repo root."
  exit 2
}

# Detect PowerShell executable name for scheduled task
$psExe = (Get-Command pwsh -ErrorAction SilentlyContinue)?.Source
if(-not $psExe){ $psExe = (Get-Command powershell -ErrorAction SilentlyContinue).Source }

# Compose wrapper
$project = 'edge'  # any stable name; containers use explicit container_name
$composeArgs = @('compose','-p', $project,'-f', $composeFile)

function Compose {
  param([Parameter(ValueFromRemainingArguments=$true)][string[]]$Args)
  & docker @composeArgs @Args
}

function Assert-Docker {
  if(-not (Get-Command docker -ErrorAction SilentlyContinue)){
    throw "Docker Desktop is not installed or 'docker' is not in PATH."
  }
  try { docker version *> $null } catch { throw "Docker is not running. Please start Docker Desktop." }
}

function Wait-Healthy([string[]]$Services, [int]$TimeoutSec = 180){
  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  foreach($svc in $Services){
    Write-Info "Waiting for service '$svc' to be healthy (timeout ${TimeoutSec}s)..."
    while($true){
      $id = Compose ps -q $svc
      if([string]::IsNullOrWhiteSpace($id)){ Start-Sleep -Seconds 2; if((Get-Date) -gt $deadline){ throw "Service '$svc' container not found." } ; continue }
      $inspect = docker inspect $id | ConvertFrom-Json
      $state = $inspect[0].State
      $health = $state.Health?.Status
      if($state.Status -ne 'running'){ Start-Sleep 2 }
      elseif([string]::IsNullOrEmpty($health) -or $health -eq 'healthy'){ Write-Ok "$svc is running$([string]::IsNullOrEmpty($health) ? '' : ' (healthy)')"; break }
      else { Start-Sleep 2 }
      if((Get-Date) -gt $deadline){ throw "Service '$svc' did not become healthy in time (last health: $health)." }
    }
  }
}

# --- Commands -----------------------------------------------------------------

function Do-Up([switch]$Build){
  Assert-Docker
  Write-Info "Starting stack (project=$project)..."
  $args = @('up','-d')
  if($Build){ $args += '--build' }
  Compose @args | Out-Host

  # Wait for infra dependencies to be healthy
  try{
    Wait-Healthy @('postgres','kafka')
  } catch {
    Write-Warn $_.Exception.Message
  }
  Write-Ok "Stack is up."
}

function Do-Down(){
  Assert-Docker
  Write-Info "Stopping stack (keeps volumes)..."
  Compose down | Out-Host
  Write-Ok "Stack is down."
}

function Do-Restart([string]$Target){
  Assert-Docker
  if([string]::IsNullOrWhiteSpace($Target)){
    Write-Info "Restarting all services..."
    Compose restart | Out-Host
  } else {
    Write-Info "Restarting service '$Target'..."
    Compose restart $Target | Out-Host
  }
}

function Do-Status(){
  Assert-Docker
  Write-Info "Containers:"
  Compose ps | Out-Host

  Write-Info "Health summary:"
  $ids = Compose ps -q
  foreach($id in $ids){
    $obj = docker inspect $id | ConvertFrom-Json
    $name = ($obj[0].Name).Trim('/')
    $state = $obj[0].State.Status
    $health = $obj[0].State.Health?.Status
    "{0,-28} state={1,-8} health={2}" -f $name, $state, ($health ?? '-') | Out-Host
  }
}

function Do-Logs([string]$Target,[switch]$Follow,[int]$Lines){
  Assert-Docker
  if([string]::IsNullOrWhiteSpace($Target)){
    Write-Info "Streaming all services logs..."
    Compose logs @('--tail', "$Lines") $(if($Follow) {'-f'}) | Out-Host
  } else {
    Write-Info "Logs for '$Target'..."
    Compose logs $Target @('--tail', "$Lines") $(if($Follow) {'-f'}) | Out-Host
  }
}

function Do-Exec([string]$Target,[string]$Cmd){
  Assert-Docker
  if([string]::IsNullOrWhiteSpace($Target)){ throw "Use -s <service> with 'exec'." }
  if([string]::IsNullOrWhiteSpace($Cmd)){ $Cmd = '/bin/sh -lc ""' }
  $id = Compose ps -q $Target
  if([string]::IsNullOrWhiteSpace($id)){ throw "Service '$Target' is not running." }
  Write-Info "Executing inside '$Target': $Cmd"
  docker exec -it $id sh -lc $Cmd
}

function Do-SeedDb(){
  Assert-Docker
  $pgId = Compose ps -q 'postgres'
  if([string]::IsNullOrWhiteSpace($pgId)){ throw "Postgres service is not running." }

  Write-Info "Re-running init.sql inside Postgres..."
  docker exec -i $pgId psql -U edge -d edgedb -f /docker-entrypoint-initdb.d/init.sql

  # Optional seed file on host repo (services/postgres/seed_data.sql)
  $seedHost = Join-Path $scriptRoot 'services\postgres\seed_data.sql'
  if(Test-Path $seedHost){
    Write-Info "Applying seed_data.sql..."
    docker cp $seedHost ${pgId}:/tmp/seed_data.sql
    docker exec -i $pgId psql -U edge -d edgedb -f /tmp/seed_data.sql
    Write-Ok "Seed data applied."
  } else {
    Write-Warn "seed_data.sql not found at services/postgres/seed_data.sql (skipping)."
  }
  Write-Ok "Database init re-applied."
}

function Do-Backup(){
  Assert-Docker
  $outDir = Join-Path $scriptRoot 'backups'
  if(-not (Test-Path $outDir)){ New-Item -ItemType Directory -Path $outDir | Out-Null }
  $ts = Get-Date -Format 'yyyyMMdd-HHmmss'

  # Postgres dump
  $pgId = Compose ps -q 'postgres'
  if($pgId){
    Write-Info "Backing up Postgres (edgedb)..."
    $pgDump = docker exec $pgId pg_dump -U edge -d edgedb
    $pgFile = Join-Path $outDir "edgedb_${ts}.sql"
    $pgDump | Out-File -FilePath $pgFile -Encoding utf8
    Write-Ok "Postgres backup: $pgFile"
  } else { Write-Warn "Postgres container not running; skip PG backup." }

  # Influx backup (best-effort; requires CLI)
  $influxId = Compose ps -q 'influxdb'
  if($influxId){
    try{
      Write-Info "Backing up InfluxDB (full instance)…"
      docker exec $influxId sh -lc 'mkdir -p /backup && influx backup /backup -t "$DOCKER_INFLUXDB_INIT_ADMIN_TOKEN"' | Out-Null
      $zip = Join-Path $outDir "influx_${ts}.zip"
      $tmp = Join-Path $outDir "influx_${ts}"
      if(Test-Path $tmp){ Remove-Item -Recurse -Force $tmp }
      New-Item -ItemType Directory -Path $tmp | Out-Null
      docker cp "${influxId}:/backup/." $tmp
      Add-Type -AssemblyName System.IO.Compression.FileSystem
      [System.IO.Compression.ZipFile]::CreateFromDirectory($tmp, $zip)
      Remove-Item -Recurse -Force $tmp
      Write-Ok "Influx backup: $zip"
    } catch {
      Write-Warn "Influx backup skipped or failed: $($_.Exception.Message)"
    }
  } else { Write-Warn "InfluxDB container not running; skip Influx backup." }
}

function Do-Prune(){
  Assert-Docker
  Write-Warn "This will remove dangling images and stopped containers (not volumes)."
  docker system prune -f | Out-Host
}

function Do-EnableAutostart(){
  $taskName = 'EdgeDevice Stack Autostart'
  $scriptPath = Join-Path $scriptRoot 'edgectl.ps1'
  $args = "-NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`" up"
  Write-Info "Creating scheduled task '$taskName' to start stack at boot…"
  $action  = New-ScheduledTaskAction -Execute $psExe -Argument $args
  $trigger = New-ScheduledTaskTrigger -AtStartup
  Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -RunLevel Highest -Force | Out-Null
  Write-Ok "Autostart enabled."
}

function Do-DisableAutostart(){
  $taskName = 'EdgeDevice Stack Autostart'
  if(Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue){
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
    Write-Ok "Autostart task removed."
  } else {
    Write-Warn "Autostart task not found."
  }
}

# --- Main dispatch ------------------------------------------------------------

switch($Command){
  'up'               { Do-Up -Build:$PSBoundParameters.ContainsKey('build') ; break }
  'build'            { Do-Up -Build:$true ; break }
  'rebuild'          { Do-Up -Build:$true ; break }
  'down'             { Do-Down ; break }
  'restart'          { Do-Restart -Target:$s ; break }
  'status'           { Do-Status ; break }
  'logs'             { Do-Logs -Target:$s -Follow:$f -Lines:$n ; break }
  'exec'             { Do-Exec -Target:$s -Cmd:$c ; break }
  'seed-db'          { Do-SeedDb ; break }
  'backup'           { Do-Backup ; break }
  'pull'             { Assert-Docker; Compose pull | Out-Host ; break }
  'prune'            { Do-Prune ; break }
  'enable-autostart' { Do-EnableAutostart ; break }
  'disable-autostart'{ Do-DisableAutostart ; break }
  default            { Do-Status }
}
