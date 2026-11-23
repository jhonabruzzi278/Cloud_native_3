package cl.duoc.ventas;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public final class StreamsApp {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(StreamsApp.class);

    private static final String STATE_STORE = "ventas-por-tienda";

    public static void main(String[] args) {
        String bootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092");
        String rawTopic = env("RAW_TOPIC", "ventas-raw");
        String aggTopic = env("AGG_TOPIC", "ventas-agregadas");
        String dlqTopic = env("DLQ_TOPIC", "ventas-raw-dlq");
        String lateTopic = env("LATE_TOPIC", "ventas-tardias");
        String applicationId = env("GROUP_ID", "ventas-aggregator-ep5");
        long windowSeconds = Long.parseLong(env("WINDOW_SECONDS", "60"));
        long graceSeconds = Long.parseLong(env("WINDOW_GRACE_SECONDS", "30"));
        long dedupHours = Long.parseLong(env("DEDUP_RETENTION_HOURS", "24"));

        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 2);
        props.put(StreamsConfig.topicPrefix(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG), 1);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.STATE_DIR_CONFIG, env("STREAMS_STATE_DIR", "/tmp/kafka-streams"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var topology = buildTopology(rawTopic, aggTopic, dlqTopic, lateTopic,
                Duration.ofSeconds(windowSeconds), Duration.ofSeconds(graceSeconds), Duration.ofHours(dedupHours));
        var streams = new KafkaStreams(topology, props);
        streams.setStateListener((now, before) -> LOG.info("estado Kafka Streams {} -> {}", before, now));
        streams.setGlobalStateRestoreListener(new RestoreLogger());
        streams.setUncaughtExceptionHandler(e -> {
            LOG.error("fallo inesperado del hilo de Streams; se reemplaza el hilo", e);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        Runtime.getRuntime().addShutdownHook(new Thread(() -> streams.close(Duration.ofSeconds(30))));

        LOG.info("Kafka Streams app={} raw={} agg={} dlq={} tardias={} ventana={}s gracia={}s dedup={}h",
                applicationId, rawTopic, aggTopic, dlqTopic, lateTopic, windowSeconds, graceSeconds, dedupHours);
        streams.start();
    }

    public static Topology buildTopology(String rawTopic, String aggTopic, String dlqTopic, String lateTopic,
                                         Duration window, Duration grace, Duration dedupRetention) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(Stores.windowStoreBuilder(
                Stores.persistentWindowStore(DeduplicationProcessor.STORE, dedupRetention, dedupRetention, false),
                Serdes.String(), Serdes.String()));
        builder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(LateEventClassifier.STORE), Serdes.String(), Serdes.Long()));

        // 1. Validacion (ruta de rechazo) y 2. deduplicacion por eventId, ambas en la particion de origen.
        KStream<String, SaleValidationProcessor.Outcome> checked = builder
                .stream(rawTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .processValues(SaleValidationProcessor::new, Named.as("validar-venta"))
                .processValues(() -> new DeduplicationProcessor(dedupRetention), Named.as("deduplicar-eventId"),
                        DeduplicationProcessor.STORE);

        checked.filter((key, outcome) -> outcome.rejection() != null, Named.as("rechazos"))
                .mapValues(SaleValidationProcessor.Outcome::rejection)
                .to(dlqTopic, Produced.with(Serdes.String(), new JsonSerde<>(SaleValidationProcessor.Rejection.class)));

        // 3. Reparticion explicita por tienda: la ventana y la politica de tardios comparten tarea y stream time.
        KStream<String, LateEventClassifier.Timed> timed = checked
                .filter((key, outcome) -> outcome.sale() != null, Named.as("ventas-validas"))
                .mapValues(SaleValidationProcessor.Outcome::sale)
                .selectKey((key, sale) -> sale.store(), Named.as("rekey-por-tienda"))
                .repartition(Repartitioned.with(Serdes.String(), new JsonSerde<>(Sale.class)).withName("por-tienda"))
                .processValues(() -> new LateEventClassifier(window, grace), Named.as("clasificar-tiempo"),
                        LateEventClassifier.STORE);

        timed.filter((store, value) -> value.late() != null, Named.as("tardias"))
                .mapValues(LateEventClassifier.Timed::late)
                .to(lateTopic, Produced.with(Serdes.String(), new JsonSerde<>(LateEventClassifier.LateSale.class)));

        KTable<Windowed<String>, TicketAggregate> aggregate = timed
                .filter((store, value) -> value.sale() != null, Named.as("a-tiempo"))
                .mapValues(LateEventClassifier.Timed::sale)
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Sale.class)))
                .windowedBy(TimeWindows.ofSizeAndGrace(window, grace))
                .aggregate(
                        TicketAggregate::new,
                        (store, sale, totals) -> totals.add(sale),
                        Materialized.<String, TicketAggregate, WindowStore<Bytes, byte[]>>as(STATE_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(TicketAggregate.class)))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()).withName("cierre-de-ventana"));

        aggregate.toStream()
                .map((windowedKey, totals) -> KeyValue.pair(
                        windowedKey.key(), resultJson(windowedKey, totals)))
                .to(aggTopic, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    private static String resultJson(Windowed<String> windowedKey, TicketAggregate totals) {
        long average = totals.count == 0 ? 0 : Math.round((double) totals.totalClp / totals.count);
        var result = new WindowResult(
                windowedKey.window().startTime().toString(),
                windowedKey.window().endTime().toString(),
                windowedKey.key(),
                totals.count,
                totals.totalClp,
                average);
        try {
            return MAPPER.writeValueAsString(result);
        } catch (Exception e) {
            throw new IllegalStateException("No se pudo serializar el agregado", e);
        }
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
