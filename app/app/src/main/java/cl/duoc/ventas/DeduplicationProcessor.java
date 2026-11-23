package cl.duoc.ventas;

import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Descarta duplicados de negocio: dos registros distintos en Kafka con el mismo eventId.
 *
 * exactly_once_v2 evita que un mismo offset de entrada se procese dos veces, pero si el productor envía dos
 * veces la misma venta, Kafka guarda dos registros con offsets distintos y ambos son "nuevos" para Streams.
 * Este procesador recuerda cada eventId en un WindowStore persistente (RocksDB + changelog) durante la
 * retención configurada y envía las repeticiones a la DLQ con la referencia al primer registro.
 */
final class DeduplicationProcessor
        extends ContextualFixedKeyProcessor<String, SaleValidationProcessor.Outcome, SaleValidationProcessor.Outcome> {
    static final String STORE = "event-ids-vistos";
    private static final Logger LOG = LoggerFactory.getLogger(DeduplicationProcessor.class);

    private final long retentionMs;
    private WindowStore<String, String> seen;

    DeduplicationProcessor(Duration retention) {
        this.retentionMs = retention.toMillis();
    }

    @Override
    public void init(FixedKeyProcessorContext<String, SaleValidationProcessor.Outcome> context) {
        super.init(context);
        seen = context.getStateStore(STORE);
    }

    @Override
    public void process(FixedKeyRecord<String, SaleValidationProcessor.Outcome> record) {
        var outcome = record.value();
        if (outcome.sale() == null) {
            context().forward(record);
            return;
        }
        String eventId = outcome.sale().eventId();
        long timestamp = record.timestamp();
        var metadata = context().recordMetadata();
        String here = metadata.map(m -> m.topic() + "-" + m.partition() + "@" + m.offset()).orElse("desconocido");

        String first = null;
        try (var previous = seen.fetch(eventId, timestamp - retentionMs, timestamp + retentionMs)) {
            if (previous.hasNext()) {
                first = previous.next().value;
            }
        }
        if (first == null) {
            seen.put(eventId, here, timestamp);
            context().forward(record);
            return;
        }

        var rejection = new SaleValidationProcessor.Rejection(outcome.originalValue(), record.key(),
                metadata.map(m -> m.topic()).orElse(null), metadata.map(m -> m.partition()).orElse(null),
                metadata.map(m -> m.offset()).orElse(null), "EVENTO_DUPLICADO",
                "eventId " + eventId + " ya fue contado en " + first);
        LOG.warn("venta duplicada reason={} eventId={} primero={} repetido={} key={}",
                rejection.reason(), eventId, first, here, record.key());
        context().forward(record.withValue(new SaleValidationProcessor.Outcome(null, rejection, outcome.originalValue())));
    }
}
