package cl.duoc.ventas;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;

final class SaleValidationProcessor
        extends ContextualFixedKeyProcessor<String, String, SaleValidationProcessor.Outcome> {
    private static final Logger LOG = LoggerFactory.getLogger(SaleValidationProcessor.class);
    private static final JsonMapper MAPPER = JsonMapper.builder()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS,
                    DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY,
                    DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES,
                    DeserializationFeature.FAIL_ON_NULL_CREATOR_PROPERTIES)
            .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
            .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
            .build();
    private static final List<String> TEXT_FIELDS = List.of(
            "eventId", "store", "storeFormat", "city", "channel", "category", "product", "customerId", "paymentMethod");

    record Outcome(Sale sale, Rejection rejection, String originalValue) { }

    record Rejection(String originalValue, String key, String sourceTopic, Integer sourcePartition,
                     Long sourceOffset, String reason, String detail) { }

    @Override
    public void process(FixedKeyRecord<String, String> record) {
        try {
            JsonNode json = parseJson(record.value());
            long timestamp = eventTimestamp(json.get("eventTime"));
            Sale sale = saleFrom(json, record.key());
            context().forward(record.withValue(new Outcome(sale, null, record.value())).withTimestamp(timestamp));
        } catch (InvalidSale e) {
            var metadata = context().recordMetadata();
            var rejection = new Rejection(record.value(), record.key(), metadata.map(m -> m.topic()).orElse(null),
                    metadata.map(m -> m.partition()).orElse(null), metadata.map(m -> m.offset()).orElse(null),
                    e.reason, e.getMessage());
            LOG.warn("venta rechazada reason={} topic={} partition={} offset={} key={} detail={}",
                    rejection.reason(), rejection.sourceTopic(), rejection.sourcePartition(), rejection.sourceOffset(),
                    rejection.key(), rejection.detail());
            context().forward(record.withValue(new Outcome(null, rejection, record.value())));
        }
    }

    private static JsonNode parseJson(String value) throws InvalidSale {
        if (value == null || value.isBlank()) {
            throw new InvalidSale("JSON_MALFORMADO", "Se esperaba un mensaje JSON no vacío");
        }
        try {
            JsonNode json = MAPPER.readTree(value);
            if (!json.isObject()) {
                throw new InvalidSale("CAMPOS_INVALIDOS", "Se esperaba un objeto JSON de venta");
            }
            return json;
        } catch (JsonProcessingException e) {
            throw new InvalidSale("JSON_MALFORMADO", "No se pudo leer un único objeto JSON sin campos duplicados");
        }
    }

    private static Sale saleFrom(JsonNode json, String key) throws InvalidSale {
        for (String field : TEXT_FIELDS) {
            JsonNode value = json.get(field);
            if (value == null || !value.isTextual() || value.textValue().isBlank()) {
                throw new InvalidSale("CAMPOS_INVALIDOS", "Campo de texto obligatorio: " + field);
            }
        }
        for (String field : List.of("quantity", "unitPriceClp", "totalClp")) {
            JsonNode value = json.get(field);
            if (value == null || !value.isIntegralNumber() || !value.canConvertToInt()) {
                throw new InvalidSale("CAMPOS_INVALIDOS", "Campo entero obligatorio de 32 bits: " + field);
            }
        }

        Sale sale;
        try {
            sale = MAPPER.treeToValue(json, Sale.class);
        } catch (JsonProcessingException e) {
            throw new InvalidSale("CAMPOS_INVALIDOS", "El objeto no coincide con el esquema de venta");
        }
        if (!sale.customerId().equals(key)) {
            throw new InvalidSale("CAMPOS_INVALIDOS", "La clave Kafka debe coincidir con customerId");
        }
        if (sale.quantity() <= 0 || sale.unitPriceClp() <= 0 || sale.totalClp() <= 0) {
            throw new InvalidSale("CAMPOS_INVALIDOS", "quantity, unitPriceClp y totalClp deben ser positivos");
        }
        if ((long) sale.quantity() * sale.unitPriceClp() != sale.totalClp()) {
            throw new InvalidSale("TOTAL_INCORRECTO", "totalClp debe ser quantity × unitPriceClp");
        }
        return sale;
    }

    private static long eventTimestamp(JsonNode value) throws InvalidSale {
        if (value != null && value.isTextual()) {
            try {
                long timestamp = Instant.parse(value.textValue()).toEpochMilli();
                if (timestamp >= 0) {
                    return timestamp;
                }
            } catch (DateTimeParseException | ArithmeticException e) {
                throw new InvalidSale("EVENT_TIME_INVALIDO", "eventTime debe representar un instante ISO 8601 válido");
            }
        }
        throw new InvalidSale("EVENT_TIME_INVALIDO", "eventTime es obligatorio y debe representar un instante desde 1970");
    }

    private static final class InvalidSale extends Exception {
        private final String reason;

        private InvalidSale(String reason, String detail) {
            super(detail);
            this.reason = reason;
        }
    }
}
