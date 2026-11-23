# Detiene el broker 2 mientras el productor envia ventas, muestra ISR antes/durante/despues y lo reinicia.
# Uso: ./scripts/probar-caida-broker.ps1 -Proyecto dsy1107-ep5
param(
    [string] $Proyecto = 'dsy1107-ep5',
    [int] $Eventos = 60,
    [string] $Evidencia = 'evidencias/06-caida-broker.txt'
)

$ErrorActionPreference = 'Continue'
Push-Location (Join-Path $PSScriptRoot '..')
$productor = $null
try {
    . (Join-Path $PSScriptRoot 'comun.ps1')
    $compose = Get-ComposeArgs $Proyecto
    $marca = { param($t) "[$((Get-Date).ToUniversalTime().ToString('HH:mm:ss'))Z] $t" }

    $salida = @('Prueba de caida del broker 2', "Proyecto Compose: $Proyecto", '')
    $salida += & $marca '== ANTES: ISR con ambos brokers =='
    $salida += Get-Isr $Proyecto
    $offsetsAntes = Invoke-Kafka $Proyecto @('kafka-get-offsets', '--bootstrap-server', $script:Bootstrap, '--topic', '^ventas-raw$')
    $salida += @('', 'Offsets de ventas-raw antes:') + $offsetsAntes

    # Productor aleatorio en segundo plano (acks=all, idempotente, reintentos infinitos).
    $productor = (& docker @($compose + @('run', '-d', '--no-deps', '-e', "PRODUCER_MAX_EVENTS=$Eventos",
        '-e', 'PRODUCE_INTERVAL=0.5', 'producer')) 2>$null | Select-Object -Last 1).Trim()
    if ($LASTEXITCODE -ne 0 -or -not $productor) { throw 'No se pudo iniciar el productor.' }
    Start-Sleep -Seconds 8

    $salida += @('', (& $marca '== Se detiene kafka-2 con el productor enviando =='))
    & docker @($compose + @('stop', 'kafka-2')) 2>$null | Out-Null
    Start-Sleep -Seconds 12
    $salida += & $marca '== DURANTE: ISR solo con el broker 1 =='
    $salida += Get-Isr $Proyecto
    $salida += @('', 'Particiones subreplicadas durante la caida:')
    $salida += Invoke-Kafka $Proyecto @('kafka-topics', '--bootstrap-server', 'kafka-1:9092', '--describe', '--under-replicated-partitions')
    $salida += @('', 'Consumer group durante la caida:') + (Get-Grupo $Proyecto)

    & docker wait $productor 2>$null | Out-Null
    # slf4j-simple escribe en stderr; cmd une ambos flujos sin que PowerShell 5.1 los convierta en errores.
    $logProductor = @(cmd /c "docker logs $productor 2>&1")
    $resumen = $logProductor | Where-Object { $_ -match 'producer detenido tras' }
    $salida += @('', (& $marca "Productor: $resumen"))
    $errores = $logProductor | Where-Object { $_ -match 'WARN|ERROR|disconnected|NOT_LEADER' } | Select-Object -First 8
    $salida += @('Advertencias del productor durante la caida (reconexion y cambio de lider):') + $errores

    $salida += @('', (& $marca '== Se reinicia kafka-2 =='))
    & docker @($compose + @('start', 'kafka-2')) 2>$null | Out-Null
    Wait-Hasta -Segundos 180 -Descripcion 'ISR completo tras reiniciar kafka-2' -Condicion {
        $sub = Invoke-Kafka $Proyecto @('kafka-topics', '--bootstrap-server', 'kafka-1:9092', '--describe',
            '--under-replicated-partitions') -PermitirFallo
        $estado = & docker inspect -f '{{.State.Health.Status}}' "$Proyecto-kafka-2-1" 2>$null
        ($estado -eq 'healthy') -and (@($sub | Where-Object { $_ -match 'Partition' }).Count -eq 0)
    }
    $salida += & $marca '== DESPUES: ISR recuperado; los lideres quedaron en el broker 1 =='
    $salida += Get-Isr $Proyecto | Where-Object { $_ -match 'ventas-raw\s+Partition' }
    # Accion operativa: devolver cada particion a su lider preferido (Kafka lo haria solo en ~5 min).
    $salida += @('', (& $marca '== Eleccion de lider preferido =='))
    $salida += Invoke-Kafka $Proyecto @('kafka-leader-election', '--bootstrap-server', $script:Bootstrap,
        '--election-type', 'preferred', '--all-topic-partitions') -PermitirFallo
    Start-Sleep -Seconds 3
    $salida += @('', (& $marca '== DESPUES: ISR completo y lideres repartidos =='))
    $salida += Get-Isr $Proyecto
    Start-Sleep -Seconds 5
    $salida += @('', 'Offsets de ventas-raw despues:')
    $salida += Invoke-Kafka $Proyecto @('kafka-get-offsets', '--bootstrap-server', $script:Bootstrap, '--topic', '^ventas-raw$')
    $salida += @('', 'Consumer group despues (LAG debe volver a 0):') + (Get-Grupo $Proyecto)
    $salida += @('', 'Logs del consumidor durante la prueba (errores/estados):')
    $salida += & docker @($compose + @('logs', '--no-color', '--since', '5m', 'consumer')) 2>$null |
        Where-Object { $_ -match 'estado Kafka Streams|ERROR|coordinator .* unavailable|Discovered group coordinator' } |
        ForEach-Object { $_ -replace 'ventas-aggregator-ep5-[0-9a-f-]{36}-', '' } | Select-Object -Last 8

    Write-Evidencia $Evidencia $salida
    $salida | Write-Output
} finally {
    if ($productor) { & docker rm -f $productor 2>$null | Out-Null }
    Pop-Location
}
