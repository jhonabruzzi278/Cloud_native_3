# Comprueba el perfil "monitoreo": targets de Prometheus, salud de Grafana y que cada consulta del dashboard
# EP5 devuelva datos en los ultimos 30 minutos. Ejecutar despues de los otros scripts para que haya trafico.
# Uso: ./scripts/verificar-monitoreo.ps1
param(
    [string] $Prometheus = 'http://127.0.0.1:9090',
    [string] $Grafana = 'http://127.0.0.1:3300',
    [string] $Evidencia = 'evidencias/02-monitoreo-grafana.txt'
)

$ErrorActionPreference = 'Stop'
Push-Location (Join-Path $PSScriptRoot '..')
try {
    . (Join-Path $PSScriptRoot 'comun.ps1')
    Add-Type -AssemblyName System.Web
    $salida = @('Monitoreo local', "Fecha UTC: $((Get-Date).ToUniversalTime().ToString('o'))", '', '== Targets de Prometheus ==')
    $targets = (Invoke-RestMethod "$Prometheus/api/v1/targets").data.activeTargets
    $salida += $targets | ForEach-Object { '{0,-14} {1,-16} {2,-5} {3}' -f $_.labels.job, $_.labels.instance, $_.health, $_.lastError }

    $salud = Invoke-RestMethod "$Grafana/api/health"
    $salida += @('', "== Grafana: database=$($salud.database) version=$($salud.version) ==")

    $fin = [DateTimeOffset]::UtcNow
    $inicio = $fin.AddMinutes(-30)
    $tablero = Get-Content -Raw -Encoding utf8 'monitoreo/grafana/dashboards/ep5-local.json' | ConvertFrom-Json
    $vacios = 0
    $salida += @('', '== Consultas del dashboard "EP5 - Kafka ventas (local)" (series o lineas en 30 min) ==')
    foreach ($panel in $tablero.panels) {
        foreach ($t in @($panel.targets)) {
            if (-not $t.expr) { continue }
            $q = [System.Web.HttpUtility]::UrlEncode($t.expr)
            if ($panel.datasource.uid -eq 'loki') {
                $url = "$Grafana/api/datasources/proxy/uid/loki/loki/api/v1/query_range?query=$q&limit=50" +
                    "&start=$($inicio.ToUnixTimeMilliseconds())000000&end=$($fin.ToUnixTimeMilliseconds())000000"
                $n = (@((Invoke-RestMethod $url).data.result) | ForEach-Object { @($_.values).Count } | Measure-Object -Sum).Sum
            } else {
                $url = "$Prometheus/api/v1/query_range?query=$q&start=$($inicio.ToUnixTimeSeconds())&end=$($fin.ToUnixTimeSeconds())&step=30"
                $n = @((Invoke-RestMethod $url).data.result).Count
            }
            if (-not $n) { $n = 0; $vacios++ }
            $estado = if ($n -gt 0) { 'OK   ' } else { 'VACIO' }
            $salida += '{0} {1,3}  {2}' -f $estado, $n, $panel.title
        }
    }

    $isr = "$Prometheus/api/v1/query_range?query=" + [System.Web.HttpUtility]::UrlEncode(
        'max by (instance) (kafka_server_ReplicaManager_Value{name="UnderReplicatedPartitions"})') +
        "&start=$($inicio.ToUnixTimeSeconds())&end=$($fin.ToUnixTimeSeconds())&step=15"
    $salida += @('', '== Maximo de particiones subreplicadas por broker en 30 min (caida de kafka-2) ==')
    foreach ($serie in (Invoke-RestMethod $isr).data.result) {
        $max = ($serie.values | ForEach-Object { [double] $_[1] } | Measure-Object -Maximum).Maximum
        $ultimo = $serie.values[-1][1]
        $salida += '{0,-14} maximo={1} actual={2}' -f $serie.metric.instance, $max, $ultimo
    }

    $abajo = @($targets | Where-Object { $_.health -ne 'up' -and $_.labels.instance -ne 'producer:9406' }).Count
    $salida += @('', "Nota: producer:9406 solo esta 'up' mientras corre 'docker compose run --use-aliases producer'.")
    $resultado = if ($abajo -eq 0 -and $vacios -eq 0) { 'Resultado: CORRECTO' } else { "Resultado: $abajo targets caidos y $vacios consultas vacias" }
    $salida += $resultado
    Write-Evidencia $Evidencia $salida
    $salida | Write-Output
    if ($abajo -gt 0 -or $vacios -gt 0) { exit 1 }
} finally {
    Pop-Location
}
