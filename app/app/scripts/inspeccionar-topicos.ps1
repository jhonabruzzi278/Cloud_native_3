# Crea (idempotente) e inspecciona topicos, configuracion, offsets y consumer groups del cluster local.
# Uso: ./scripts/inspeccionar-topicos.ps1 -Proyecto dsy1107-ep5
param(
    [string] $Proyecto = 'dsy1107-ep5',
    [string] $Evidencia = 'evidencias/05-topicos-y-grupos.txt'
)

$ErrorActionPreference = 'Continue'
Push-Location (Join-Path $PSScriptRoot '..')
try {
    . (Join-Path $PSScriptRoot 'comun.ps1')
    $compose = Get-ComposeArgs $Proyecto

    # El servicio topics usa --if-not-exists: repetirlo no cambia topicos existentes.
    & docker @($compose + @('run', '--rm', '--no-deps', 'topics')) 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'No se pudieron crear los topicos.' }

    $topicos = @('ventas-raw', 'ventas-agregadas', 'ventas-raw-dlq', 'ventas-tardias')
    $salida = @('Topicos y consumer groups', "Proyecto Compose: $Proyecto",
        "Fecha UTC: $((Get-Date).ToUniversalTime().ToString('o'))", '', '== Topicos de negocio (particiones, RF, ISR) ==')
    $salida += Get-Isr $Proyecto
    $salida += @('', '== Configuracion explicita por topico ==')
    foreach ($t in $topicos) {
        $salida += Invoke-Kafka $Proyecto @('kafka-configs', '--bootstrap-server', $script:Bootstrap, '--describe',
            '--entity-type', 'topics', '--entity-name', $t)
    }
    $salida += @('', '== Configuracion de broker relevante (broker 1) ==')
    $salida += Invoke-Kafka $Proyecto @('kafka-configs', '--bootstrap-server', $script:Bootstrap, '--describe',
        '--entity-type', 'brokers', '--entity-name', '1', '--all') |
        Where-Object { $_ -match 'min.insync.replicas=|default.replication.factor=|auto.create.topics.enable=|offsets.topic.replication.factor=|transaction.state.log.(replication.factor|min.isr)=|unclean.leader.election.enable=' }
    $salida += @('', '== Topicos internos de Kafka Streams ==')
    $salida += Invoke-Kafka $Proyecto @('kafka-topics', '--bootstrap-server', $script:Bootstrap, '--describe',
        '--topic', 'ventas-aggregator-ep5-.*') | Where-Object { $_ -match '^Topic:' }
    $salida += @('', '== Offsets finales (topico:particion:offset) ==')
    $salida += Invoke-Kafka $Proyecto @('kafka-get-offsets', '--bootstrap-server', $script:Bootstrap,
        '--topic-partitions', 'ventas-raw:0-3,ventas-agregadas:0-1,ventas-raw-dlq:0-3,ventas-tardias:0-1')
    $salida += @('', '== Consumer groups ==')
    $salida += Invoke-Kafka $Proyecto @('kafka-consumer-groups', '--bootstrap-server', $script:Bootstrap, '--list')
    $salida += @('')
    $salida += Get-Grupo $Proyecto
    $salida += @('', '== Particiones subreplicadas (vacio = todas con ISR completo) ==')
    $salida += Invoke-Kafka $Proyecto @('kafka-topics', '--bootstrap-server', $script:Bootstrap, '--describe',
        '--under-replicated-partitions')

    Write-Evidencia $Evidencia $salida
    $salida | Write-Output
} finally {
    Pop-Location
}
