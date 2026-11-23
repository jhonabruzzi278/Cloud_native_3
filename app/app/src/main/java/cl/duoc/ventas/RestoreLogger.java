package cl.duoc.ventas;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Deja en el log cuando un state store se reconstruye desde su changelog (evidencia de restauracion). */
final class RestoreLogger implements StateRestoreListener {
    private static final Logger LOG = LoggerFactory.getLogger(RestoreLogger.class);

    @Override
    public void onRestoreStart(TopicPartition partition, String store, long startOffset, long endOffset) {
        LOG.info("restauracion iniciada store={} changelog={} desde={} hasta={} pendientes={}",
                store, partition, startOffset, endOffset, endOffset - startOffset);
    }

    @Override
    public void onBatchRestored(TopicPartition partition, String store, long batchEndOffset, long numRestored) {
    }

    @Override
    public void onRestoreEnd(TopicPartition partition, String store, long totalRestored) {
        LOG.info("restauracion terminada store={} changelog={} registros={}", store, partition, totalRestored);
    }
}
