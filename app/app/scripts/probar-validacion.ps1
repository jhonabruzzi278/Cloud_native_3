param(
    [string] $Proyecto = 'dsy1107-validacion',
    [string] $Evidencia = 'evidencias/02-validacion-dlq.txt'
)

$ErrorActionPreference = 'Stop'
Push-Location (Join-Path $PSScriptRoot '..')
try {
    $compose = @('compose', '-p', $Proyecto, '--env-file', '.env.example')
    $broker = $compose + @('exec', '-T', 'kafka-1')
    $bootstrap = @('--bootstrap-server', 'kafka-1:9092,kafka-2:9092')

    $offsets = & docker @broker kafka-get-offsets @bootstrap --topic '^ventas-raw$'
    if ($LASTEXITCODE -ne 0 -or @($offsets).Count -ne 4) { throw 'No se pudieron consultar las cuatro particiones.' }
    if ($offsets -match ':[1-9][0-9]*$') { throw 'Usa un proyecto Compose nuevo para ejecutar la prueba con tópicos vacíos.' }

    $key = 'cliente-prueba'
    $start = [DateTimeOffset]::UtcNow.AddMinutes(-5)
    $start = $start.AddTicks(-($start.Ticks % [TimeSpan]::TicksPerMinute))
    $sale = @{
        eventId = 'valida-antes'; eventTime = $start.AddSeconds(10).ToString('o'); store = 'Tienda Prueba'
        storeFormat = 'tienda'; city = 'Santiago'; channel = 'presencial'; category = 'ropa'; product = 'Polera'
        quantity = 2; unitPriceClp = 1000; totalClp = 2000; customerId = $key; paymentMethod = 'debito'
    }
    $missing = $sale.Clone()
    $missing.Remove('store')
    $wrong = $sale.Clone()
    $wrong.totalClp = 1234
    $badTime = $sale.Clone()
    $badTime.eventTime = 'ayer'
    $after = $sale.Clone()
    $after.eventId = 'valida-despues'
    $after.eventTime = $start.AddSeconds(20).ToString('o')
    $after.unitPriceClp = 1250
    $after.totalClp = 2500
    $close = $sale.Clone()
    $close.eventId = 'avance-de-ventana'
    $close.eventTime = $start.AddSeconds(95).ToString('o')
    $invalid = @(
        '{"eventId":',
        ($missing | ConvertTo-Json -Compress),
        ($wrong | ConvertTo-Json -Compress),
        ($badTime | ConvertTo-Json -Compress)
    )
    $records = @(($sale | ConvertTo-Json -Compress)) + $invalid + @(
        ($after | ConvertTo-Json -Compress), ($close | ConvertTo-Json -Compress)
    )

    $records | ForEach-Object { "$key|$_" } | & docker @broker kafka-console-producer @bootstrap --topic ventas-raw --producer-property acks=all --property parse.key=true --property 'key.separator=|'
    if ($LASTEXITCODE -ne 0) { throw 'No se pudieron publicar los casos de prueba.' }

    $dlq = & docker @broker kafka-console-consumer @bootstrap --topic ventas-raw-dlq --from-beginning --max-messages 4 --timeout-ms 25000 --isolation-level read_committed
    if ($LASTEXITCODE -ne 0 -or @($dlq).Count -ne 4) { throw 'Se esperaban cuatro rechazos confirmados.' }
    $reasons = @('JSON_MALFORMADO', 'CAMPOS_INVALIDOS', 'TOTAL_INCORRECTO', 'EVENT_TIME_INVALIDO')
    $rejected = $dlq | ForEach-Object { ConvertFrom-Json -InputObject $_ } | Sort-Object sourceOffset
    for ($index = 0; $index -lt $reasons.Count; $index++) {
        $item = $rejected[$index]
        if ($item.reason -ne $reasons[$index] -or $item.originalValue -cne $invalid[$index] -or
            $item.key -ne $key -or $item.sourceTopic -ne 'ventas-raw' -or $item.sourceOffset -ne ($index + 1)) {
            throw "La DLQ no conservó los datos esperados para $($reasons[$index])."
        }
    }

    $output = & docker @broker kafka-console-consumer @bootstrap --topic ventas-agregadas --from-beginning --max-messages 1 --timeout-ms 25000 --isolation-level read_committed
    if ($LASTEXITCODE -ne 0 -or @($output).Count -ne 1) { throw 'Se esperaba un agregado confirmado.' }
    $result = ConvertFrom-Json -InputObject $output -AsHashtable -DateKind String
    if ($result.store -ne $sale.store -or $result.sales_count -ne 2 -or $result.total_clp -ne 4500 -or
        $result.avg_ticket_clp -ne 2250 -or [DateTimeOffset]::Parse($result.window_start) -ne $start -or
        [DateTimeOffset]::Parse($result.window_end) -ne $start.AddMinutes(1)) {
        throw 'El agregado no coincide con las dos ventas válidas y el tiempo de evento esperado.'
    }

    $topic = & docker @broker kafka-topics @bootstrap --describe --topic ventas-raw-dlq
    if ($LASTEXITCODE -ne 0) { throw 'No se pudo describir la DLQ.' }
    $logs = & docker @compose logs --no-color consumer | Select-String -SimpleMatch 'venta rechazada reason='
    if (@($logs).Count -ne 4) { throw 'Se esperaban cuatro logs correlacionables con la DLQ.' }

    $directory = Split-Path -Parent $Evidencia
    if ($directory) { New-Item -ItemType Directory -Force -Path $directory | Out-Null }
    @('Prueba de validación y tiempo de evento', "Proyecto Compose: $Proyecto", '', 'Entradas en orden:') +
        $records + @('', 'Configuración Kafka de la DLQ:') + $topic + @('', 'Rechazos confirmados:') +
        $dlq + @('', 'Logs del consumidor:') + @($logs | ForEach-Object { $_.Line }) +
        @('', 'Agregado confirmado:', $output, '', 'Resultado: 4 rechazos y 2 ventas válidas en la ventana.') |
        Set-Content -Encoding utf8 -LiteralPath $Evidencia
    Write-Output "Validación correcta. Evidencia: $Evidencia"
} finally {
    Pop-Location
}
