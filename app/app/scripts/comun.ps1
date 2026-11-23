# Funciones compartidas por los scripts de prueba. Compatibles con Windows PowerShell 5.1 y PowerShell 7.
# Se cargan con:  . (Join-Path $PSScriptRoot 'comun.ps1')

$script:Bootstrap = 'kafka-1:9092,kafka-2:9092'

function Get-ComposeArgs([string] $Proyecto) {
    return @('compose', '-p', $Proyecto, '--env-file', '.env.example')
}

# Ejecuta un comando dentro de un contenedor del proyecto y devuelve stdout como lineas.
# stderr se descarta para que PowerShell 5.1 no lo convierta en errores; el codigo de salida se revisa aparte.
function Invoke-EnServicio([string] $Proyecto, [string] $Servicio, [string[]] $Comando, [switch] $PermitirFallo) {
    $argumentos = (Get-ComposeArgs $Proyecto) + @('exec', '-T', $Servicio) + $Comando
    $salida = & docker @argumentos 2>$null
    if ($LASTEXITCODE -ne 0 -and -not $PermitirFallo) {
        throw "Fallo ($LASTEXITCODE): docker $($argumentos -join ' ')"
    }
    return @($salida)
}

function Invoke-Kafka([string] $Proyecto, [string[]] $Comando, [string] $Broker = 'kafka-1', [switch] $PermitirFallo) {
    return Invoke-EnServicio $Proyecto $Broker $Comando -PermitirFallo:$PermitirFallo
}

# Lee todos los registros confirmados de un topico (clave|valor) hasta que no lleguen mas en TimeoutMs.
function Read-Topico([string] $Proyecto, [string] $Topico, [int] $TimeoutMs = 15000, [string] $Broker = 'kafka-1') {
    $bootstrap = if ($Broker -eq 'kafka-2') { 'kafka-2:9092' } else { $script:Bootstrap }
    return Invoke-Kafka $Proyecto @('kafka-console-consumer', '--bootstrap-server', $bootstrap, '--topic', $Topico,
        '--from-beginning', '--timeout-ms', "$TimeoutMs", '--isolation-level', 'read_committed',
        '--property', 'print.key=true', '--property', 'key.separator=|') -Broker $Broker -PermitirFallo
}

function Split-Registro([string] $Linea) {
    $i = $Linea.IndexOf('|')
    return [pscustomobject]@{ Key = $Linea.Substring(0, $i); Value = $Linea.Substring($i + 1) }
}

function Get-Isr([string] $Proyecto, [string] $Broker = 'kafka-1') {
    $bootstrap = if ($Broker -eq 'kafka-2') { 'kafka-2:9092' } else { $script:Bootstrap }
    return Invoke-Kafka $Proyecto @('kafka-topics', '--bootstrap-server', $bootstrap, '--describe',
        '--topic', 'ventas-raw|ventas-agregadas|ventas-raw-dlq|ventas-tardias') -Broker $Broker
}

function Get-Grupo([string] $Proyecto, [string] $Grupo = 'ventas-aggregator-ep5', [string] $Broker = 'kafka-1') {
    $bootstrap = if ($Broker -eq 'kafka-2') { 'kafka-2:9092' } else { $script:Bootstrap }
    return Invoke-Kafka $Proyecto @('kafka-consumer-groups', '--bootstrap-server', $bootstrap, '--describe',
        '--group', $Grupo) -Broker $Broker -PermitirFallo
}

# Verdadero cuando el grupo tiene miembros activos y todas sus particiones con offset tienen LAG 0.
function Test-SinLag([string] $Proyecto, [string] $Grupo = 'ventas-aggregator-ep5') {
    $filas = @(Get-Grupo $Proyecto $Grupo | Where-Object { $_ -match "^$Grupo\s" } | ForEach-Object { ,($_ -split '\s+') })
    if ($filas.Count -eq 0) { return $false }
    foreach ($f in $filas) {
        if ($f[6] -eq '-') { return $false }
        if ($f[5] -ne '-' -and $f[5] -ne '0') { return $false }
    }
    return $true
}

function Write-Evidencia([string] $Ruta, [object[]] $Lineas) {
    $directorio = Split-Path -Parent $Ruta
    if ($directorio) { New-Item -ItemType Directory -Force -Path $directorio | Out-Null }
    $Lineas | ForEach-Object { "$_" } | Out-File -Encoding utf8 -FilePath $Ruta
}

function Wait-Hasta([scriptblock] $Condicion, [int] $Segundos = 90, [string] $Descripcion = 'condicion') {
    $limite = (Get-Date).AddSeconds($Segundos)
    while ((Get-Date) -lt $limite) {
        if (& $Condicion) { return }
        Start-Sleep -Seconds 3
    }
    throw "Tiempo agotado esperando: $Descripcion"
}
