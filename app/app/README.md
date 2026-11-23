# Streaming resiliente de ventas · DSY1107 (EP5)

Flujo de ventas en tiempo real con Kafka (dos brokers y ZooKeeper) y Kafka Streams en Java 21: validación con DLQ, deduplicación por `eventId`, ventanas tumbling por tienda sobre `eventTime` con período de gracia, ruta separada para eventos tardíos y pruebas de caída y recuperación.

## 1. Arquitectura

El [diagrama](arquitectura_encargo.mmd) muestra los componentes, los tópicos, las claves, las particiones, la replicación y la ruta de la DLQ.

```
Productor ──(clave customerId)──▶ ventas-raw ──▶ Kafka Streams ventas-aggregator-ep5
                                                  1 validar-venta ─────────────▶ ventas-raw-dlq (inválidos)
                                                  2 deduplicar-eventId ────────▶ ventas-raw-dlq (EVENTO_DUPLICADO)
                                                  ── repartición por store ──
                                                  3 clasificar-tiempo ─────────▶ ventas-tardias (fuera de gracia)
                                                  4 ventana 60 s + gracia 30 s ─▶ ventas-agregadas
```

| Pieza | Decisión | Justificación |
| --- | --- | --- |
| Clave en `ventas-raw` | `customerId` | Mantiene en orden las ventas de un mismo cliente y hace que el reenvío de una venta caiga en la misma partición, donde vive su `eventId` en el store de deduplicación. Reparte mejor que `store`, que solo tiene 7 valores. |
| Clave de agregación | `store` (repartición explícita `por-tienda`) | La ventana agrupa por tienda. La repartición deja en una misma tarea la política de tardíos y la ventana, por lo que ambas ven el mismo tiempo observado. |
| Clave en `ventas-agregadas` y `ventas-tardias` | `store` | Un consumidor puede leer en orden todo lo de una tienda. |
| Clave en `ventas-raw-dlq` | La clave original | Se puede reinyectar el registro corregido con la misma clave y la misma partición. |
| Garantía | `exactly_once_v2` | Offsets, estado y salidas se confirman en una sola transacción. |

Los DTO son records Java tipados: `Sale` (entrada), `WindowResult` (salida), `Rejection` (DLQ) y `LateSale` (tardíos). Todos usan JSON.

## 2. Prerrequisitos

- Docker Desktop con Docker Compose v2 en modo de contenedores Linux. Maven y Java 21 se ejecutan dentro de Docker, así que no hace falta instalarlos.
- Windows PowerShell 5.1 o PowerShell 7 para los scripts de `scripts/`. `probar-validacion.ps1`, el primer script de la EP, requiere PowerShell 7.
- Unos 3 GB de RAM libres para ZooKeeper, los dos brokers y la aplicación, y 1 GB más con el perfil `monitoreo`.

## 3. Levantar el sistema

Todos los comandos se ejecutan desde esta carpeta, la que contiene `compose.yaml`. Usa un nombre de proyecto para aislar tópicos y volúmenes.

```powershell
docker compose -p dsy1107-ep5 --env-file .env.example config --quiet
docker compose -p dsy1107-ep5 --env-file .env.example up -d --build
docker compose -p dsy1107-ep5 --env-file .env.example ps -a
```

`up --build` compila la imagen y ejecuta `mvn verify`, de modo que la imagen solo se construye si pasan las pruebas. El servicio `topics` crea los tópicos con `--if-not-exists` y termina con código 0. `consumer` es Kafka Streams. El productor está en el perfil `demo` y se lanza aparte:

```powershell
# flujo normal: 80 ventas aleatorias
docker compose -p dsy1107-ep5 --env-file .env.example run --rm --no-deps --use-aliases -e PRODUCER_MAX_EVENTS=80 producer
# ver una venta con su clave y partición, y un agregado confirmado
docker compose -p dsy1107-ep5 --env-file .env.example exec -T kafka-1 kafka-console-consumer --bootstrap-server kafka-1:9092 --topic ventas-raw --from-beginning --max-messages 1 --property print.key=true --property print.partition=true
docker compose -p dsy1107-ep5 --env-file .env.example exec -T kafka-1 kafka-console-consumer --bootstrap-server kafka-1:9092 --topic ventas-agregadas --from-beginning --max-messages 3 --isolation-level read_committed --property print.key=true
```

`--use-aliases` registra el contenedor como `producer` en la red, y así Prometheus puede leer sus métricas.

Para levantar además Prometheus, Loki, Alloy y Grafana (sección 10), agrega el perfil `monitoreo`:

```powershell
docker compose -p dsy1107-ep5 --env-file .env.example --profile monitoreo up -d --build
```

Para apagarlo: `docker compose -p dsy1107-ep5 --profile monitoreo down`. Con `down -v` también se borran los datos.

### Variables configurables

Todas tienen un valor por defecto en `compose.yaml`, y [.env.example](.env.example) las documenta sin secretos. Se pueden cambiar con `-e VAR=valor` en `run` o en un `.env` local, que está ignorado por git.

| Variable | Defecto | Efecto |
| --- | --- | --- |
| `GROUP_ID` | `ventas-aggregator-ep5` | `application.id` de Streams, que también es el consumer group y el prefijo de los tópicos internos |
| `WINDOW_SECONDS` | `60` | Tamaño de la ventana tumbling |
| `WINDOW_GRACE_SECONDS` | `30` | Período de gracia para eventos fuera de orden |
| `DEDUP_RETENTION_HOURS` | `24` | Tiempo durante el que se recuerda un `eventId` |
| `PRODUCER_MODE` | `aleatorio` | `escenarios` publica la secuencia controlada de la sección 7 |
| `PRODUCER_MAX_EVENTS` | `0` | Cantidad de ventas en modo aleatorio (0 = sin límite) |
| `PRODUCE_INTERVAL` | `1.0` | Segundos entre ventas, con ±30 % de variación |
| `CUSTOMER_POOL` | `200` | Cantidad de clientes distintos |
| `SCENARIO_STORE` | `Tienda Escenarios` | Tienda que usa el modo escenarios |
| `GRAFANA_PORT` | `3300` | Puerto local de Grafana, solo en `127.0.0.1` |
| `PROMETHEUS_PORT` | `9090` | Puerto local de Prometheus, solo en `127.0.0.1` |
| `GRAFANA_ADMIN_PASSWORD` | sin defecto | Contraseña del usuario `admin` del Grafana local. El ejemplo trae un marcador que conviene cambiar en un `.env` propio. |

`KAFKA_BOOTSTRAP` y los nombres de los tópicos (`RAW_TOPIC`, `AGG_TOPIC`, `DLQ_TOPIC`, `LATE_TOPIC`) se definen en el bloque `x-app-env` de `compose.yaml`.

## 4. Tópicos y configuración del clúster

| Tópico | Particiones | RF | `min.insync.replicas` | Retención | Motivo |
| --- | ---: | ---: | ---: | --- | --- |
| `ventas-raw` | 4 | 2 | 1 | 7 días | Paralelismo de hasta 4 hilos o instancias de Streams; permite reprocesar una semana. |
| `ventas-agregadas` | 2 | 2 | 1 | 7 días | Bajo volumen, un registro por tienda y por minuto. |
| `ventas-raw-dlq` | 4 | 2 | 1 | 14 días | Más tiempo para inspeccionar, corregir y reinyectar. |
| `ventas-tardias` | 2 | 2 | 1 | 14 días | Mismo criterio que la DLQ: requieren revisión manual. |
| Internos de Streams | 4 | 2 | 1 | compactados o según la ventana | `replication.factor=2` y `topic.min.insync.replicas=1` se definen en `StreamsApp`. |

Configuración de los brokers en `compose.yaml`: `default.replication.factor=2`, `min.insync.replicas=1`, `offsets.topic.replication.factor=2`, `transaction.state.log.replication.factor=2`, `transaction.state.log.min.isr=1` y `auto.create.topics.enable=false`. Esta última evita que un error de tipeo cree un tópico con RF 1.

**Por qué RF 2 y no 3:** el factor de replicación no puede superar la cantidad de brokers. Con dos brokers, RF 3 falla al crear el tópico (`InvalidReplicationFactorException`). Con RF 2, cada partición tiene una copia en cada broker.

**Por qué `min.insync.replicas=1`:** con RF 2 y `acks=all`, un valor de 2 rechazaría las escrituras (`NotEnoughReplicas`) apenas cayera un broker. Con 1, el sistema sigue escribiendo con un solo broker. El costo es que, mientras el ISR tiene una sola réplica, una escritura confirmada existe en un solo disco. Si ese broker se pierde antes de que el otro vuelva y replique, esos mensajes se pierden. En este clúster se privilegia la disponibilidad. Con tres brokers se usaría RF 3 y `min.insync.replicas=2`.

Script repetible para crear e inspeccionar tópicos, configuración, offsets y consumer groups:

```powershell
./scripts/inspeccionar-topicos.ps1 -Proyecto dsy1107-ep5
```

Resultado: [05-topicos-y-grupos.txt](evidencias/05-topicos-y-grupos.txt).

## 5. Validación, logs y DLQ

`SaleValidationProcessor` valida que el mensaje sea un único objeto JSON sin claves duplicadas, que existan los campos de texto y los enteros obligatorios, que `customerId` coincida con la clave Kafka, que `quantity`, `unitPriceClp` y `totalClp` sean positivos, que `totalClp = quantity × unitPriceClp` y que `eventTime` sea un instante ISO 8601 desde 1970.

Un registro inválido no lanza excepción, así que el hilo sigue procesando. El registro va a `ventas-raw-dlq` con esta forma:

```json
{"originalValue":"{\"eventId\":","key":"cliente-escenarios","sourceTopic":"ventas-raw","sourcePartition":0,"sourceOffset":1,"reason":"JSON_MALFORMADO","detail":"No se pudo leer un único objeto JSON sin campos duplicados"}
```

Motivos posibles: `JSON_MALFORMADO`, `CAMPOS_INVALIDOS`, `TOTAL_INCORRECTO`, `EVENT_TIME_INVALIDO` y `EVENTO_DUPLICADO`. Cada rechazo deja también un log `WARN venta rechazada reason=… topic=… partition=… offset=… key=…`. Con tópico, partición y offset se puede cruzar el log con la DLQ y con el registro original en `ventas-raw`.

**Reinyectar un registro corregido:** se lee `originalValue` desde la DLQ, se corrige el JSON y se publica en `ventas-raw` con la misma clave (`kafka-console-producer --property parse.key=true`). Si se usa el mismo `eventId` y la venta original había sido rechazada, no hay conflicto con la deduplicación, porque los registros rechazados no se guardan como vistos.

## 6. Duplicados y semántica de entrega

**Qué garantiza `exactly_once_v2`:** el consumo de un offset, los cambios de estado y los registros que produce se confirman en una sola transacción. Si Streams se cae a la mitad, la transacción se aborta y el offset se reprocesa sin dejar salidas ni estado duplicados. El productor también es idempotente (`enable.idempotence=true`, `acks=all`), por lo que un reintento de red no duplica el registro en el log.

**Qué no garantiza:** si la aplicación de origen envía dos veces la misma venta, por ejemplo porque el usuario repite el pago o porque el productor se reinicia y vuelve a enviar, Kafka guarda dos registros con offsets distintos. Para EOS son dos entradas legítimas y ambas se contarían.

**Solución:** `DeduplicationProcessor` guarda cada `eventId` en un `WindowStore` persistente (RocksDB) llamado `event-ids-vistos`, con changelog RF 2. Guarda también la referencia `tópico-partición@offset` del primer registro. Una repetición dentro de `DEDUP_RETENTION_HOURS` (24 h) se envía a la DLQ como `EVENTO_DUPLICADO`, con el detalle `eventId X ya fue contado en ventas-raw-0@5`, y no llega a la agregación.

- Se eligió una retención de 24 h porque cubre reintentos y reenvíos del mismo día con un estado acotado: un `eventId` por venta. La ventana de duplicados es mucho mayor que la de agregación porque un duplicado puede llegar mucho después.
- El store vive en la tarea de la partición de `ventas-raw`. Como la clave es `customerId`, un reenvío de la misma venta llega a la misma partición. Un duplicado con otro `customerId` no se detectaría, pero esa combinación ya es una venta distinta para el negocio.
- El estado sobrevive a los reinicios porque se reconstruye desde su changelog. Esto se demuestra en la sección 9.

## 7. Tiempo de evento, ventanas y tardíos

- **Tiempo de evento:** la validación reemplaza el timestamp del registro por `eventTime`. Las ventanas usan la hora en que ocurrió la venta, no la hora de llegada a Kafka.
- **Ventana:** tumbling de 60 s alineada al minuto. Permite ver la venta por tienda casi en tiempo real sin crear demasiados registros.
- **Gracia:** 30 s. Absorbe los reintentos del productor, el cambio de líder durante la caída de un broker (unos segundos según la sección 9) y diferencias pequeñas entre relojes. Un valor mayor retrasa cada resultado, porque una ventana solo se publica cuando cierra en `fin + gracia`.
- **Publicación:** `suppress(untilWindowCloses)` (store `cierre-de-ventana`) emite un solo resultado final por tienda y ventana, con `window_start`, `window_end`, `sales_count`, `total_clp` y `avg_ticket_clp`.
- **Tiempo observado:** el tiempo del flujo avanza con el mayor `eventTime` visto en cada partición de `por-tienda`. Kafka Streams no usa el reloj de pared. Una ventana cierra cuando llega a esa partición un evento posterior a `fin + gracia`.

**Política para eventos fuera de orden** (`LateEventClassifier`, store `tiempo-observado` con changelog):

| Situación | Condición | Qué pasa | Cómo se detecta |
| --- | --- | --- | --- |
| Dentro de gracia | `fin de su ventana + 30 s > tiempo observado` | Se suma a su ventana original | Log `INFO venta fuera de orden aceptada dentro de gracia` |
| Fuera de gracia | `fin + 30 s <= tiempo observado` | No se suma. Va a `ventas-tardias` con la ventana, el cierre y el tiempo observado | Tópico `ventas-tardias`, log `WARN venta tardia fuera de gracia` y métrica de Streams `dropped-records` |

Un evento tardío es una venta válida. Nunca se clasifica como JSON inválido ni va a la DLQ. Si el negocio lo requiere, se puede reconciliar después desde `ventas-tardias`.

## 8. Pruebas automatizadas

[StreamsAppTest](src/test/java/cl/duoc/ventas/StreamsAppTest.java) usa `TopologyTestDriver`, así que no necesita brokers. Tiene 6 pruebas:

| Prueba | Requisito |
| --- | --- |
| `rejectsMalformedJsonWithOriginalValueAndMetadata` | DLQ con valor original, clave, tópico, partición, offset y motivo |
| `rejectsMissingFieldsIncorrectTotalAndInvalidTime` | Validación de campos, total y `eventTime` |
| `aggregatesValidSalesUsingEventTimeAfterRejectingAnInvalidRecord` | El flujo continúa tras un error. Agregado por tienda con `eventTime` |
| `repeatedEventIdIsCountedOnceAndSentToDlqWithReference` | Deduplicación por `eventId` |
| `outOfOrderSaleInsideGraceIsAddedAndAfterGraceGoesToLateTopic` | Escenario temporal dentro y fuera de gracia |
| `controlledScenarioSequenceProducesExpectedOutputs` | La secuencia completa del modo escenarios |

Cómo ejecutarlas:

```powershell
docker compose --progress plain --env-file .env.example build consumer
# o solo las pruebas, sin construir la imagen:
docker run --rm -v "${PWD}:/src" -w /src maven:3.9-eclipse-temurin-21 mvn -B -ntp test
```

Resultado: `Tests run: 6, Failures: 0, Errors: 0` ([03-pruebas-streams.txt](evidencias/03-pruebas-streams.txt)).

## 9. Matriz de escenarios

Con el sistema levantado (sección 3), cada script publica sus datos, comprueba el resultado y guarda la evidencia. Si una comprobación falla, el script termina con código 1.

| Escenario | Comando | Resultado esperado | Evidencia obtenida |
| --- | --- | --- | --- |
| Flujo normal | `run producer` (sección 3) | Venta con clave `customerId` en una partición. Agregado por tienda al cerrar la ventana. | [01](evidencias/01-laboratorio-base.txt): 80 envíos, lag 0, agregado de Paris Costanera con 2 ventas. |
| JSON malformado, campo ausente, total incorrecto, `eventTime` inválido | `./scripts/probar-escenarios.ps1` | El consumidor sigue activo. 4 registros en la DLQ con el original y su motivo. 4 logs correlacionables. | [04](evidencias/04-escenarios-ep5.txt): `JSON_MALFORMADO`, `CAMPOS_INVALIDOS`, `TOTAL_INCORRECTO` y `EVENT_TIME_INVALIDO`, con partición y offset. |
| Duplicado de `eventId` | mismo script, caso `DUPLICADO` | Se cuenta una sola vez y la DLQ recibe `EVENTO_DUPLICADO` con referencia al primer registro | [04](evidencias/04-escenarios-ep5.txt): primera ventana con 3 ventas y 7.500 CLP. Sin el filtro serían 4 ventas y 10.000 CLP. |
| Tardío dentro de gracia | caso `TARDIO_DENTRO_DE_GRACIA` (T+30 s cuando el tiempo observado es T+70 s) | Se suma a la ventana [T, T+60) | [04](evidencias/04-escenarios-ep5.txt): incluido en las 3 ventas. Log `INFO … dentro de gracia`. |
| Tardío fuera de gracia | caso `TARDIO_FUERA_DE_GRACIA` (T+40 s cuando el tiempo observado es T+100 s) | No se suma. Queda en `ventas-tardias` y no en la DLQ. | [04](evidencias/04-escenarios-ep5.txt): venta de 9.000 CLP en `ventas-tardias`, `windowClosedAt` = T+90 s. |
| Broker 2 detenido mientras se produce | `./scripts/probar-caida-broker.ps1` | El ISR baja a `[1]` y luego vuelve a `[1,2]`. El productor termina sin errores. Lag 0. | [06](evidencias/06-caida-broker.txt): 60 de 60 ventas confirmadas (offsets de 112 a 172) y elección de líder preferido. |
| Reinicio del consumidor con estado borrado | `./scripts/probar-reinicio-consumidor.ps1 -BorrarEstado` | Retoma desde los offsets confirmados y reconstruye los stores desde los changelogs. Un `eventId` previo se sigue detectando como duplicado. | [07](evidencias/07-reinicio-consumidor.txt): lag 30 → 0, logs `restauracion terminada … registros=N` y `EVENTO_DUPLICADO` tras el reinicio. |
| Topología, configuración y grupos | `./scripts/inspeccionar-topicos.ps1` | Particiones, RF, ISR, retención, tópicos internos, offsets y lag | [05](evidencias/05-topicos-y-grupos.txt) |

Todos los scripts aceptan `-Proyecto <nombre>`. El script de escenarios usa una tienda con sufijo horario y fija la base T en el minuto siguiente. Como el tiempo observado es por partición, conviene detener el productor aleatorio mientras se ejecuta y esperar unos 4 minutos antes de repetirlo en el mismo proyecto. Si no, la ejecución anterior ya adelantó el tiempo observado de esa partición.

### Detalle de la caída del broker

1. **Antes:** las 4 particiones de `ventas-raw` tienen `Isr: 1,2`. Los líderes se reparten entre ambos brokers.
2. **Durante:** con `kafka-2` detenido, todas las particiones quedan con `Leader: 1` e `Isr: 1`. `--under-replicated-partitions` lista todas las particiones. El productor (`acks=all`, `min.insync.replicas=1`) sigue confirmando escrituras y solo registra `Node -2 disconnected`. Kafka Streams pierde a su coordinador de grupo (`kafka-2`), encuentra otro en `kafka-1` en segundos y mantiene el lag en 0.
3. **Después:** `kafka-2` vuelve a estar healthy y copia desde el líder lo que no tenía. El ISR vuelve a `1,2` en menos de 10 s. Los líderes quedan todos en el broker 1 hasta que se ejecuta `kafka-leader-election --election-type preferred`, que el script lanza como acción operativa. Kafka también lo haría solo en unos 5 minutos (`auto.leader.rebalance.enable`).
4. **Límite:** con un solo broker vivo no queda redundancia. Si `kafka-1` hubiera caído en ese momento, las escrituras sin replicar se habrían perdido (sección 4).

### Detalle del reinicio del consumidor

Mientras Streams está detenido, el consumer group conserva sus offsets confirmados y el lag crece. En la evidencia, 30 ventas quedaron como lag 10/5/6/9. Al arrancar sin el volumen `streams-state`, cada store se reconstruye leyendo su changelog desde el offset 0 (`restauracion iniciada … pendientes=N`) antes de pasar a `RUNNING`. Después se procesan las ventas pendientes y el lag vuelve a 0. No hay pérdida silenciosa: lo que no se confirmó se reprocesa, y por EOS sus salidas parciales se descartaron. Con el volumen intacto, la restauración solo lee desde el último checkpoint y es casi inmediata.

## 10. Observabilidad y operación

| Señal | Comando o fuente | Acción operativa |
| --- | --- | --- |
| ISR y particiones subreplicadas | `kafka-topics --describe --under-replicated-partitions` | Si no está vacío, revisar el broker que falta y su disco. Mientras tanto no hay redundancia. |
| Consumer lag | `kafka-consumer-groups --describe --group ventas-aggregator-ep5` | Si crece de forma sostenida, revisar errores y throughput. Escalar con más instancias o `num.stream.threads`, hasta 4 por las particiones. Si no alcanza, aumentar particiones. |
| Asignación de particiones | columnas `CONSUMER-ID` y `HOST` del mismo comando | Detectar instancias caídas o rebalanceos continuos. |
| DLQ y tardíos | `kafka-get-offsets` sobre `ventas-raw-dlq` y `ventas-tardias` | Si suben, revisar al productor de origen, sus datos o su reloj. Ajustar la gracia si los tardíos son frecuentes y legítimos. |
| Estado de Streams | logs `estado Kafka Streams A -> B` y `restauracion …` | Si se queda en `REBALANCING` o `ERROR`, revisar los brokers y el estado local. |

### Monitoreo local con Grafana

Cada JVM publica sus MBeans en HTTP `/metrics` mediante el agente JMX Exporter 1.0.1 (Maven Central, regla `.*` en [monitoreo/jmx/config.yml](monitoreo/jmx/config.yml)). Los puertos son 9404 en los brokers, 9405 en ZooKeeper, 9406 en el productor y 9407 en Streams. El agente viene dentro de la imagen de la app, y el servicio `jmx-agent` lo copia a un volumen que montan ZooKeeper y los brokers. En esos dos servicios, `KAFKA_OPTS` se exporta solo para el proceso del servidor: así el healthcheck y los `docker compose exec kafka-1 kafka-…` no intentan abrir un segundo exporter en el mismo puerto.

Con el perfil `monitoreo` (sección 3):

| Componente | Acceso | Contenido |
| --- | --- | --- |
| Grafana | http://127.0.0.1:3300 (anónimo con rol Editor, que permite usar Explore) | Dashboard de inicio **EP5 - Kafka ventas (local)**, en la carpeta *Ventas Lab* |
| Prometheus | http://127.0.0.1:9090/targets | Jobs `kafka-brokers`, `zookeeper` y `ventas-app` ([monitoreo/prometheus.yml](monitoreo/prometheus.yml)) |
| Loki + Alloy | desde Grafana (fuente *Loki*) | Logs de los contenedores de este proyecto Compose ([monitoreo/alloy.alloy](monitoreo/alloy.alloy)) |

El [dashboard local](monitoreo/grafana/dashboards/ep5-local.json) es el *EA3 – Observabilidad integral* del curso, con las IP de EC2 reemplazadas por los nombres de servicio Docker. Además agrega una fila **EP5** con estos paneles:

- mensajes por segundo en `ventas-raw`, `ventas-agregadas`, `ventas-raw-dlq` y `ventas-tardias`;
- lag de Streams por partición;
- particiones subreplicadas y bajo `min.insync` por broker, junto con la cantidad de líderes;
- registros procesados y descartados por Streams (`dropped-records`);
- logs de rechazos y duplicados;
- logs de eventos tardíos, fuera de orden y restauraciones.

Verificación (23-11-2026):

- Los 6 targets de Prometheus quedaron `up`.
- Todas las consultas del dashboard devolvieron datos después de ejecutar `probar-escenarios.ps1` y `probar-caida-broker.ps1`.
- Durante la caída de `kafka-2`, el panel de ISR mostró el salto de `UnderReplicatedPartitions` en `kafka-1`, que volvió a 0 al reiniciar.

Los indicadores *Exporters disponibles (de 5)* y *Procesos de la app (de 2)* cuentan también al productor, que solo existe mientras corre `docker compose run`. Fuera de esas ejecuciones marcan 4 y 1, en rojo.

La versión EC2 del curso se despliega aparte con la carpeta `monitoring/`, siguiendo las guías *Despliegue Prometheus/Grafana* y *Scrapeo Kafka/ZooKeeper/app*, y usa el mismo agente con `JAVA_TOOL_OPTIONS`.

**Métricas de negocio:** no hay contadores Prometheus propios para rechazos por motivo ni para tardíos. Para mostrarlos se usa:

- la tasa de mensajes por tópico de `ventas-raw-dlq` y `ventas-tardias`, en el panel de la fila EP5;
- los logs `WARN` en Loki, por ejemplo `{service="consumer"} |= "venta rechazada"`;
- la métrica JMX de Streams `kafka_streams_stream_task_metrics_dropped_records`.

## 11. Límites conocidos

- **ZooKeeper tiene un solo nodo y es un punto único de falla.** Si cae, los brokers siguen sirviendo lecturas y escrituras a los líderes actuales, pero no se pueden elegir nuevos líderes, crear tópicos ni actualizar el ISR. Una caída de broker mientras ZooKeeper está caído deja sus particiones sin líder. Se necesitarían 3 nodos de ZooKeeper o migrar a KRaft.
- Con dos brokers y RF 2 se tolera la caída de **un** broker. Con `min.insync.replicas=1`, durante la degradación no hay redundancia (sección 4).
- En esta instalación local, los cuatro contenedores comparten un mismo host: la prueba simula la caída de un proceso, no la de una máquina. En la versión EC2 de la guía, cada broker está en su propia instancia.
- El tiempo observado es por partición de `por-tienda`. Una tienda con el reloj adelantado puede cerrar ventanas de otras tiendas en la misma partición y volverlas tardías.
- La deduplicación cubre 24 h y requiere que el duplicado conserve la misma clave `customerId`.
- `suppress` con buffer ilimitado guarda en memoria las ventanas abiertas: una por tienda activa en los últimos 90 s. Es poco con este volumen.
- `num.standby.replicas=0`: si se reemplaza una instancia sin su volumen, hay que restaurar desde el changelog. En la evidencia tomó unos segundos.
- La primera evidencia ([01](evidencias/01-laboratorio-base.txt)) es anterior a la deduplicación y a la gracia de 30 s, y usó el grupo `ventas-aggregator` del laboratorio. La segunda ([02](evidencias/02-validacion-dlq.txt)) usó gracia 0. Las evidencias 03 a 07 corresponden a la versión final.

## 12. Recuperación

| Falla | Pasos |
| --- | --- |
| Broker caído | `docker compose -p <p> --env-file .env.example start kafka-2`, esperar `healthy` y confirmar que `kafka-topics --describe --under-replicated-partitions` está vacío. Luego ejecutar `kafka-leader-election --bootstrap-server kafka-1:9092 --election-type preferred --all-topic-partitions`. |
| Kafka Streams caído o reiniciado | `docker compose -p <p> --env-file .env.example up -d consumer` y esperar el log `-> RUNNING`. Verificar lag 0 con `kafka-consumer-groups`. |
| Estado local corrupto | `docker compose -p <p> rm -sf consumer` y `docker volume rm <p>_streams-state`, luego `up -d consumer`. Los stores se reconstruyen desde los changelogs. |
| ZooKeeper caído | `docker compose -p <p> --env-file .env.example start zookeeper`. Los brokers se reconectan solos. Revisar el ISR. |
| Broker sale con `NodeExists` al recrear ZooKeeper | ZooKeeper todavía conserva el registro efímero de la sesión anterior. Los brokers tienen `restart: on-failure`, así que se reintentan solos cuando la sesión expira (unos 18 s). Si no, ejecutar `up -d` de nuevo. |
| Registros en la DLQ | Revisar `reason` y `detail`, corregir el JSON y volver a publicarlo en `ventas-raw` con la misma clave (sección 5). |
| Reprocesar desde cero (cambio de topología) | Detener `consumer` y ejecutar `kafka-streams-application-reset --application-id ventas-aggregator-ep5 --input-topics ventas-raw --bootstrap-server kafka-1:9092` dentro de `kafka-1`. Luego borrar el volumen `streams-state` y volver a levantarlo. Otra opción es usar un `GROUP_ID` nuevo. |

## 13. Secretos

El repositorio no tiene credenciales. `.env.example` solo trae parámetros funcionales, y `.gitignore` excluye `.env` y `*.env` (excepto el ejemplo). El clúster local usa `PLAINTEXT` sin autenticación y no publica puertos de Kafka al host, así que todo el acceso es por `docker compose exec`. Grafana y Prometheus solo escuchan en `127.0.0.1`. `GRAFANA_ADMIN_PASSWORD` en `.env.example` es un marcador local; para cualquier otro uso, define una contraseña propia en un `.env` que no se versiona. En la versión EC2, la contraseña de Grafana va en `monitoring.env`, que no se versiona.
