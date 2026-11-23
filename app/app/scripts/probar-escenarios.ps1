# Publica la secuencia controlada de la EP5 (valida, invalidas, duplicado, tardio en gracia y fuera de gracia)
# con el productor Java en modo "escenarios" y compara DLQ, ventas-agregadas y ventas-tardias con lo esperado.
# Uso: ./scripts/probar-escenarios.ps1 -Proyecto dsy1107-ep5
# Detener antes el productor aleatorio: el tiempo observado es por particion y otras tiendas lo pueden adelantar.
param(
    [string] $Proyecto = 'dsy1107-ep5',
    [string] $Evidencia = 'evidencias/04-escenarios-ep5.txt'
)

$ErrorActionPreference = 'Continue'
Push-Location (Join-Path $PSScriptRoot '..')
try {
    . (Join-Path $PSScriptRoot 'comun.ps1')
    $tienda = 'Tienda Escenarios ' + (Get-Date).ToUniversalTime().ToString('HHmmss')

    $compose = Get-ComposeArgs $Proyecto
    $producidos = & docker @($compose + @('run', '--rm', '--no-deps', '--use-aliases', '-e', 'PRODUCER_MODE=escenarios',
        '-e', "SCENARIO_STORE=$tienda", 'producer')) 2>$null
    if ($LASTEXITCODE -ne 0) { throw 'El productor de escenarios fallo.' }
    $producidos = @($producidos | Where-Object { $_ -match 'particion=' })
    if ($producidos.Count -ne 12) { throw "Se esperaban 12 envios y hubo $($producidos.Count)." }

    # particion/offset de cada caso en ventas-raw, para cruzar con la DLQ
    $origen = @{}
    foreach ($linea in $producidos) {
        if ($linea -match '^(\S+)\s+particion=(\d+) offset=(\d+)') { $origen["$($Matches[2])@$($Matches[3])"] = $Matches[1] }
    }

    $agregados = @()
    Wait-Hasta -Segundos 90 -Descripcion 'dos agregados de la tienda de escenarios' -Condicion {
        $script:agregados = @(Read-Topico $Proyecto 'ventas-agregadas' 8000 | Where-Object { $_.StartsWith("$tienda|") })
        $script:agregados.Count -ge 2
    }
    $agregados = $script:agregados | ForEach-Object { (Split-Registro $_).Value | ConvertFrom-Json }

    $dlq = @(Read-Topico $Proyecto 'ventas-raw-dlq' 8000 | ForEach-Object { (Split-Registro $_).Value | ConvertFrom-Json } |
        Where-Object { $origen.ContainsKey("$($_.sourcePartition)@$($_.sourceOffset)") })
    $tardias = @(Read-Topico $Proyecto 'ventas-tardias' 8000 | Where-Object { $_.StartsWith("$tienda|") } |
        ForEach-Object { (Split-Registro $_).Value | ConvertFrom-Json })

    $errores = @()
    $esperado = @{ JSON_MALFORMADO = 'JSON_MALFORMADO'; CAMPO_AUSENTE = 'CAMPOS_INVALIDOS'
        TOTAL_INCORRECTO = 'TOTAL_INCORRECTO'; EVENT_TIME_INVALIDO = 'EVENT_TIME_INVALIDO'; DUPLICADO = 'EVENTO_DUPLICADO' }
    foreach ($r in $dlq) {
        $caso = $origen["$($r.sourcePartition)@$($r.sourceOffset)"]
        if ($esperado[$caso] -ne $r.reason) { $errores += "DLQ: caso $caso con motivo $($r.reason)" }
        if (-not $r.originalValue -or $r.key -ne 'cliente-escenarios' -or $r.sourceTopic -ne 'ventas-raw') {
            $errores += "DLQ: el caso $caso no conserva valor original, clave o topico"
        }
    }
    if ($dlq.Count -ne 5) { $errores += "DLQ: se esperaban 5 rechazos y hubo $($dlq.Count)" }
    if ($agregados[0].sales_count -ne 3 -or $agregados[0].total_clp -ne 7500 -or $agregados[0].avg_ticket_clp -ne 2500) {
        $errores += 'Ventana 1: se esperaban 3 ventas, 7.500 CLP y ticket 2.500 (duplicado excluido, tardio en gracia incluido)'
    }
    if ($agregados[1].sales_count -ne 2 -or $agregados[1].total_clp -ne 12000) {
        $errores += 'Ventana 2: se esperaban 2 ventas y 12.000 CLP'
    }
    if ($tardias.Count -ne 1 -or $tardias[0].sale.totalClp -ne 9000 -or $tardias[0].reason -ne 'FUERA_DE_GRACIA') {
        $errores += 'ventas-tardias: se esperaba solo la venta de 9.000 CLP fuera de gracia'
    }

    $logs = & docker @($compose + @('logs', '--no-color', 'consumer')) 2>$null |
        Select-String -SimpleMatch -Pattern 'cliente-escenarios', $tienda | ForEach-Object { $_.Line }
    $grupo = Get-Grupo $Proyecto

    $resultado = if ($errores.Count -eq 0) { 'Resultado: CORRECTO' } else { @('Resultado: CON DIFERENCIAS') + $errores }
    Write-Evidencia $Evidencia (@(
        'Escenarios controlados EP5', "Proyecto Compose: $Proyecto", "Tienda: $tienda",
        "Fecha UTC: $((Get-Date).ToUniversalTime().ToString('o'))", '',
        '== Envios a ventas-raw (caso, particion, offset, clave, valor) ==') + $producidos + @('',
        '== ventas-raw-dlq (rechazos de esta ejecucion) ==') + ($dlq | ForEach-Object { $_ | ConvertTo-Json -Compress }) + @('',
        '== ventas-agregadas ==') + ($agregados | ForEach-Object { $_ | ConvertTo-Json -Compress }) + @('',
        '== ventas-tardias ==') + ($tardias | ForEach-Object { $_ | ConvertTo-Json -Compress -Depth 4 }) + @('',
        '== Logs del consumidor ==') + $logs + @('', '== Consumer group ==') + $grupo + @('') + $resultado)

    $resultado | Write-Output
    Write-Output "Evidencia: $Evidencia"
    if ($errores.Count -gt 0) { exit 1 }
} finally {
    Pop-Location
}
