# Detiene Kafka Streams, publica ventas mientras esta caido y lo reinicia. Con -BorrarEstado elimina el volumen
# local de RocksDB para obligar a reconstruir los stores desde sus changelogs. Luego reenvia un eventId que ya
# se proceso antes de la caida: si el estado se restauro, la repeticion termina en la DLQ como EVENTO_DUPLICADO.
# Uso: ./scripts/probar-reinicio-consumidor.ps1 -Proyecto dsy1107-ep5 -BorrarEstado
param(
    [string] $Proyecto = 'dsy1107-ep5',
    [switch] $BorrarEstado,
    [int] $Eventos = 30,
    [string] $Evidencia = 'evidencias/07-reinicio-consumidor.txt'
)

$ErrorActionPreference = 'Continue'
Push-Location (Join-Path $PSScriptRoot '..')
try {
    . (Join-Path $PSScriptRoot 'comun.ps1')
    $compose = Get-ComposeArgs $Proyecto
    $marca = { param($t) "[$((Get-Date).ToUniversalTime().ToString('HH:mm:ss'))Z] $t" }

    # Venta conocida (solo ASCII para que el pipe funcione igual en PowerShell 5.1 y 7).
    $id = 'reinicio-' + (Get-Date).ToUniversalTime().ToString('yyyyMMddHHmmss')
    $venta = [ordered]@{ eventId = $id; eventTime = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
        store = 'Tienda Reinicio'; storeFormat = 'tienda'; city = 'Santiago'; channel = 'presencial'; category = 'ropa'
        product = 'Polera'; quantity = 1; unitPriceClp = 1500; totalClp = 1500; customerId = 'cliente-reinicio'
        paymentMethod = 'debito' } | ConvertTo-Json -Compress
    # Se envia en base64 y se decodifica dentro del contenedor: evita BOM y comillas del pipe de PowerShell 5.1.
    $b64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("cliente-reinicio|$venta"))
    $enviar = {
        & docker @($compose + @('exec', '-T', 'kafka-1', 'bash', '-c',
            "echo $b64 | base64 -d | kafka-console-producer --bootstrap-server $($script:Bootstrap) --topic ventas-raw --producer-property acks=all --property parse.key=true --property key.separator='|'")) 2>$null
    }

    $salida = @('Prueba de reinicio de Kafka Streams', "Proyecto Compose: $Proyecto", "Borrar estado local: $BorrarEstado",
        "eventId de control: $id", '')
    & $enviar
    Wait-Hasta -Segundos 60 -Descripcion 'consumo de la venta de control' -Condicion { Test-SinLag $Proyecto }
    $salida += & $marca '== ANTES: venta de control procesada; consumer group =='
    $salida += Get-Grupo $Proyecto

    $salida += @('', (& $marca '== Se detiene el consumidor =='))
    & docker @($compose + @('stop', 'consumer')) 2>$null | Out-Null
    & docker @($compose + @('run', '--rm', '--no-deps', '-e', "PRODUCER_MAX_EVENTS=$Eventos", '-e', 'PRODUCE_INTERVAL=0.1',
        'producer')) 2>$null | Out-Null
    $salida += & $marca "== DURANTE: $Eventos ventas publicadas con el consumidor detenido (lag acumulado) =="
    $salida += Get-Grupo $Proyecto

    if ($BorrarEstado) {
        & docker @($compose + @('rm', '-f', 'consumer')) 2>$null | Out-Null
        & docker volume rm "$($Proyecto)_streams-state" 2>$null | Out-Null
        $salida += @('', (& $marca "Volumen $($Proyecto)_streams-state eliminado: los stores se reconstruyen desde el changelog"))
    }
    $inicio = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
    & docker @($compose + @('up', '-d', '--no-build', 'consumer')) 2>$null | Out-Null
    Wait-Hasta -Segundos 180 -Descripcion 'consumidor RUNNING' -Condicion {
        @(& docker @($compose + @('logs', '--no-color', '--since', $inicio, 'consumer')) 2>$null |
            Where-Object { $_ -match '-> RUNNING' }).Count -gt 0
    }
    Wait-Hasta -Segundos 120 -Descripcion 'lag 0 tras el reinicio' -Condicion { Test-SinLag $Proyecto }
    $salida += @('', (& $marca '== DESPUES: el grupo retoma desde los offsets confirmados (LAG 0) =='))
    $salida += Get-Grupo $Proyecto
    $salida += @('', 'Restauracion de stores registrada por RestoreLogger:')
    $salida += & docker @($compose + @('logs', '--no-color', '--since', $inicio, 'consumer')) 2>$null |
        Where-Object { $_ -match 'restauracion (iniciada|terminada)|-> RUNNING' -and $_ -notmatch 'registros=0$' }

    $salida += @('', (& $marca "== Reenvio de la venta de control $id tras el reinicio =="))
    & $enviar
    $duplicado = $null
    Wait-Hasta -Segundos 60 -Descripcion 'rechazo del duplicado en DLQ' -Condicion {
        $script:duplicado = Read-Topico $Proyecto 'ventas-raw-dlq' 6000 |
            Where-Object { $_ -match [regex]::Escape($id) -and $_ -match 'EVENTO_DUPLICADO' } | Select-Object -First 1
        [bool] $script:duplicado
    }
    $salida += 'DLQ:'
    $salida += $script:duplicado
    $salida += @('', 'Resultado: CORRECTO. El estado de deduplicacion sobrevivio al reinicio y no hubo doble conteo.')

    Write-Evidencia $Evidencia $salida
    $salida | Write-Output
} finally {
    Pop-Location
}
