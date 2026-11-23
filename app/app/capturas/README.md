# Capturas para la presentación (EP6) y el informe (EP5)

Las capturas son del 23-11-2026 (13:14–13:24 UTC), sobre el stack recreado desde cero con el que se generaron las evidencias 01 a 07. Las de terminal (`t*`) reproducen la salida real de los archivos de `evidencias/`; solo se recortaron columnas y se resaltaron los valores clave.

| Archivo | Qué muestra | Criterio de la rúbrica |
| --- | --- | --- |
| `a02-flujo-simplificado.png` | Flujo de una venta: productor → ventas-raw → validar → deduplicar → tiempo → ventana → agregados, con ramas a DLQ y tardías | Arquitectura (EP6 #1) |
| `a01-arquitectura.png` | `arquitectura_encargo.mmd` completo: tópicos, claves, particiones, RF y changelogs | Diagrama actualizado (entregable EP5) |
| `t02-topicos-rf-isr.png` | Particiones, RF 2, `min.insync.replicas=1`, retención y configuración del broker | Configuración de tópicos (EP5 #1, EP6 #2) |
| `t01-flujo-normal.png` | Venta con clave `customerId` y partición, agregados por tienda y lag 0 | Flujo normal (EP5 #2, EP6 #2) |
| `t03-escenarios-envios.png` | Los 12 casos controlados del generador, con partición y offset | Datos de prueba reproducibles (EP5 req. 6) |
| `t04-dlq-rechazos.png` | DLQ con motivo, clave, offset, detalle y valor original | Validación y DLQ (EP5 #4, EP6 #3) |
| `t06-logs-consumidor.png` | Logs `WARN` de rechazo, duplicado y tardío, y log `INFO` del evento fuera de orden | Logs contextuales (EP5 #4) |
| `g07-logs-dlq-duplicados.png` | Los mismos logs consultados en Grafana/Loki | Logs en observabilidad (EP6 #3) |
| `t05-agregados-y-tardios.png` | Ventana de 3 ventas por $7.500 (duplicado excluido, tardío en gracia incluido) y venta fuera de gracia en `ventas-tardias` | Duplicados y tiempo de evento (EP5 #3 y #5, EP6 #4 y #5) |
| `t07-caida-broker-isr.png` | ISR antes, durante y después de detener `kafka-2`; 60 ventas sin pérdida | Tolerancia a fallos (EP5 #6, EP6 #6) |
| `g02-isr-caida-broker.png` | Pico de `UnderReplicatedPartitions` (132) en `kafka-1` durante la caída y vuelta a 0 | Tolerancia a fallos en métricas (EP6 #6) |
| `g03-subreplicadas-ea3.png` | Mismo evento en el panel original del dashboard EA3 | Tolerancia a fallos (EP6 #6) |
| `t08-reinicio-lag.png` | Lag antes (0), durante (7/9/7/7) y después (0) del reinicio de Streams | Consumer lag y recuperación (EP6 #6 y #7) |
| `t09-restauracion-changelog.png` | Stores restaurados desde changelog y duplicado detectado después del reinicio | Restauración de estado (EP5 #5 y #6) |
| `g06-streams-procesados.png` | Registros procesados por segundo; el hueco de las 13:20 es el reinicio | Recuperación (EP6 #6) |
| `g05-lag-por-particion.png` | Lag por partición en Grafana: 0 sostenido, con el hueco del reinicio | Supervisión (EP6 #7) |
| `g04-mensajes-por-topico.png` | Mensajes por segundo en `ventas-raw`, `ventas-agregadas`, DLQ y tardías | Métricas operativas (EP6 #7) |
| `g01-dashboard-general.png` | Vista general del dashboard: exporters, brokers, tráfico y JVM | Monitoreo (EP6 preparación) |
| `g09-trafico-brokers.png`, `g10-zookeeper.png` | Tráfico de los brokers y conexiones de ZooKeeper | Monitoreo |
| `p01-prometheus-targets.png` | Los 6 targets de Prometheus en estado `UP` | Monitoreo |
| `t11-prometheus-targets.png` | Resultado de `verificar-monitoreo.ps1`: 31 de 31 consultas con datos | Monitoreo |
| `t10-pruebas-automatizadas.png` | `Tests run: 6, Failures: 0` y `BUILD SUCCESS` | Pruebas (EP5 #7) |

En `g01`, los indicadores "de 5" y "de 2" aparecen en rojo porque en ese momento el productor no estaba corriendo; solo existe mientras se ejecuta `docker compose run`.
