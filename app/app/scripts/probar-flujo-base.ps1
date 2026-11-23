# Flujo normal: publica ventas aleatorias, muestra una venta con clave y particion, un agregado confirmado,
# el consumer group y el ISR. Uso: ./scripts/probar-flujo-base.ps1 -Proyecto dsy1107-ep5
param(
    [string] $Proyecto = 'dsy1107-ep5',
    [int] $Eventos = 160,
    [string] $Evidencia = 'evidencias/01-flujo-base.txt'
)

$ErrorActionPreference = 'Continue'
Push-Location (Join-Path $PSScriptRoot '..')
try {
    . (Join-Path $PSScriptRoot 'comun.ps1')
    $compose = Get-ComposeArgs $Proyecto
    $salida = @('Flujo normal', "Proyecto Compose: $Proyecto", "Fecha UTC: $((Get-Date).ToUniversalTime().ToString('o'))", '')

    $salida += '== ISR inicial =='
    $salida += Get-Isr $Proyecto

    $producidas = & docker @($compose + @('run', '--rm', '--no-deps', '--use-aliases', '-e', "PRODUCER_MAX_EVENTS=$Eventos",
        '-e', 'PRODUCE_INTERVAL=0.5', 'producer')) 2>$null
    if ($LASTEXITCODE -ne 0) { throw 'El productor fallo.' }
    $salida += @('', "== Productor: $(@($producidas | Where-Object { $_ -match 'detenido tras' }))", 'Primeras 3 ventas enviadas:')
    $salida += $producidas | Where-Object { $_.StartsWith('{') } | Select-Object -First 3

    $salida += @('', '== Una venta en ventas-raw (particion, clave, valor) ==')
    $salida += Invoke-Kafka $Proyecto @('kafka-console-consumer', '--bootstrap-server', $script:Bootstrap, '--topic', 'ventas-raw',
        '--from-beginning', '--max-messages', '3', '--timeout-ms', '15000', '--property', 'print.partition=true',
        '--property', 'print.key=true') -PermitirFallo

    $agregados = @()
    Wait-Hasta -Segundos 150 -Descripcion 'agregados por tienda' -Condicion {
        $script:agregados = @(Read-Topico $Proyecto 'ventas-agregadas' 8000)
        $script:agregados.Count -ge 3
    }
    $salida += @('', "== ventas-agregadas (clave store | resultado de la ventana): $($script:agregados.Count) registros, primeros 5 ==")
    $salida += $script:agregados | Select-Object -First 5

    Wait-Hasta -Segundos 60 -Descripcion 'lag 0' -Condicion { Test-SinLag $Proyecto }
    $salida += @('', '== Consumer group (offsets, lag y asignacion de particiones) ==')
    $salida += Get-Grupo $Proyecto | ForEach-Object { $_ -replace 'ventas-aggregator-ep5-[0-9a-f-]{36}-', '' }
    $salida += @('', 'Resultado: CORRECTO. Ventas confirmadas, agregados por tienda publicados y lag 0.')

    Write-Evidencia $Evidencia $salida
    $salida | Write-Output
} finally {
    Pop-Location
}
