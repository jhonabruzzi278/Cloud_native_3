package cl.duoc.ventas;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamsAppTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String CUSTOMER = "cliente-prueba";
    private static final Instant BASE = Instant.parse("2026-01-01T00:00:00Z");
    private static final Instant KAFKA_TIMESTAMP = BASE.plusSeconds(600);

    @TempDir
    Path stateDir;

    private TopologyTestDriver driver;
    private TestInputTopic<String, String> input;
    private TestOutputTopic<String, String> dlq;
    private TestOutputTopic<String, String> aggregates;
    private TestOutputTopic<String, String> late;

    @BeforeEach
    void startTopology() {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ventas-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
        driver = new TopologyTestDriver(StreamsApp.buildTopology("raw", "agg", "dlq", "late",
                Duration.ofSeconds(60), Duration.ofSeconds(30), Duration.ofHours(24)), props, BASE);
        input = driver.createInputTopic("raw", new StringSerializer(), new StringSerializer());
        dlq = driver.createOutputTopic("dlq", new StringDeserializer(), new StringDeserializer());
        aggregates = driver.createOutputTopic("agg", new StringDeserializer(), new StringDeserializer());
        late = driver.createOutputTopic("late", new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void closeTopology() {
        if (driver != null) {
            driver.close();
        }
    }

    @Test
    void rejectsMalformedJsonWithOriginalValueAndMetadata() throws Exception {
        String original = "{\"eventId\":";
        input.pipeInput(CUSTOMER, original, KAFKA_TIMESTAMP);
        var record = dlq.readKeyValue();
        var rejection = MAPPER.readTree(record.value);

        assertEquals(CUSTOMER, record.key);
        assertEquals(CUSTOMER, rejection.get("key").asText());
        assertEquals(original, rejection.get("originalValue").asText());
        assertEquals("raw", rejection.get("sourceTopic").asText());
        assertEquals(0, rejection.get("sourcePartition").intValue());
        assertEquals(0L, rejection.get("sourceOffset").longValue());
        assertEquals("JSON_MALFORMADO", rejection.get("reason").asText());
        assertTrue(aggregates.isEmpty());
    }

    @Test
    void rejectsMissingFieldsIncorrectTotalAndInvalidTime() throws Exception {
        ObjectNode missingStore = MAPPER.valueToTree(sale("missing", 10, 1000));
        missingStore.remove("store");
        input.pipeInput(CUSTOMER, missingStore.toString(), KAFKA_TIMESTAMP);

        ObjectNode wrongTotal = MAPPER.valueToTree(sale("wrong", 20, 1000));
        wrongTotal.put("totalClp", 1234);
        input.pipeInput(CUSTOMER, wrongTotal.toString(), KAFKA_TIMESTAMP);

        ObjectNode invalidTime = MAPPER.valueToTree(sale("bad-time", 30, 1000));
        invalidTime.put("eventTime", "ayer");
        input.pipeInput(CUSTOMER, invalidTime.toString(), KAFKA_TIMESTAMP);

        assertEquals("CAMPOS_INVALIDOS", MAPPER.readTree(dlq.readValue()).get("reason").asText());
        assertEquals("TOTAL_INCORRECTO", MAPPER.readTree(dlq.readValue()).get("reason").asText());
        assertEquals("EVENT_TIME_INVALIDO", MAPPER.readTree(dlq.readValue()).get("reason").asText());
        assertTrue(dlq.isEmpty());
        assertTrue(aggregates.isEmpty());
    }

    @Test
    void aggregatesValidSalesUsingEventTimeAfterRejectingAnInvalidRecord() throws Exception {
        input.pipeInput(CUSTOMER, MAPPER.writeValueAsString(sale("before", 10, 1000)), KAFKA_TIMESTAMP);
        input.pipeInput(CUSTOMER, "{", KAFKA_TIMESTAMP);
        input.pipeInput(CUSTOMER, MAPPER.writeValueAsString(sale("after", 20, 1250)), KAFKA_TIMESTAMP);
        assertTrue(aggregates.isEmpty());

        input.pipeInput(CUSTOMER, MAPPER.writeValueAsString(sale("close", 95, 1000)), KAFKA_TIMESTAMP);
        var record = aggregates.readKeyValue();
        var result = MAPPER.readTree(record.value);

        assertEquals("Tienda Prueba", record.key);
        assertEquals(BASE.toString(), result.get("window_start").asText());
        assertEquals(BASE.plusSeconds(60).toString(), result.get("window_end").asText());
        assertEquals(2, result.get("sales_count").intValue());
        assertEquals(4500, result.get("total_clp").intValue());
        assertEquals(2250, result.get("avg_ticket_clp").intValue());
        assertTrue(aggregates.isEmpty());
        assertEquals("JSON_MALFORMADO", MAPPER.readTree(dlq.readValue()).get("reason").asText());
    }

    @Test
    void repeatedEventIdIsCountedOnceAndSentToDlqWithReference() throws Exception {
        String sale = MAPPER.writeValueAsString(sale("dup", 10, 1000));
        input.pipeInput(CUSTOMER, sale, KAFKA_TIMESTAMP);
        input.pipeInput(CUSTOMER, sale, KAFKA_TIMESTAMP);
        input.pipeInput(CUSTOMER, MAPPER.writeValueAsString(sale("close", 95, 1000)), KAFKA_TIMESTAMP);

        var rejection = MAPPER.readTree(dlq.readValue());
        assertEquals("EVENTO_DUPLICADO", rejection.get("reason").asText());
        assertEquals(sale, rejection.get("originalValue").asText());
        assertEquals(1L, rejection.get("sourceOffset").longValue());
        assertTrue(rejection.get("detail").asText().contains("raw-0@0"));
        assertTrue(dlq.isEmpty());

        var result = MAPPER.readTree(aggregates.readValue());
        assertEquals(1, result.get("sales_count").intValue());
        assertEquals(2000, result.get("total_clp").intValue());
    }

    @Test
    void outOfOrderSaleInsideGraceIsAddedAndAfterGraceGoesToLateTopic() throws Exception {
        input.pipeInput(CUSTOMER, MAPPER.writeValueAsString(sale("w0", 10, 1000)), KAFKA_TIMESTAMP);
        input.pipeInput(CUSTOMER, MAPPER.writeValueAsString(sale("w1", 70, 1000)), KAFKA_TIMESTAMP);
        input.pipeInput(CUSTOMER, MAPPER.writeValueAsString(sale("en-gracia", 30, 500)), KAFKA_TIMESTAMP);
        assertTrue(aggregates.isEmpty());

        input.pipeInput(CUSTOMER, MAPPER.writeValueAsString(sale("cierra-w0", 100, 1000)), KAFKA_TIMESTAMP);
        input.pipeInput(CUSTOMER, MAPPER.writeValueAsString(sale("fuera-de-gracia", 40, 4000)), KAFKA_TIMESTAMP);

        var closed = MAPPER.readTree(aggregates.readValue());
        assertEquals(BASE.toString(), closed.get("window_start").asText());
        assertEquals(2, closed.get("sales_count").intValue());
        assertEquals(3000, closed.get("total_clp").intValue());

        var record = late.readKeyValue();
        var tardy = MAPPER.readTree(record.value);
        assertEquals("Tienda Prueba", record.key);
        assertEquals("FUERA_DE_GRACIA", tardy.get("reason").asText());
        assertEquals("fuera-de-gracia", tardy.get("eventId").asText());
        assertEquals(BASE.plusSeconds(90).toString(), tardy.get("windowClosedAt").asText());
        assertEquals(BASE.plusSeconds(100).toString(), tardy.get("streamTime").asText());
        assertTrue(late.isEmpty());
        assertTrue(dlq.isEmpty(), "un evento tardio no es un dato invalido");
        assertTrue(aggregates.isEmpty());
    }

    @Test
    void controlledScenarioSequenceProducesExpectedOutputs() throws Exception {
        for (var scenario : Scenarios.build(BASE, "Tienda Escenarios")) {
            input.pipeInput(scenario.key(), scenario.value(), KAFKA_TIMESTAMP);
        }

        List<String> reasons = new ArrayList<>();
        while (!dlq.isEmpty()) {
            reasons.add(MAPPER.readTree(dlq.readValue()).get("reason").asText());
        }
        assertEquals(List.of("JSON_MALFORMADO", "CAMPOS_INVALIDOS", "TOTAL_INCORRECTO", "EVENT_TIME_INVALIDO",
                "EVENTO_DUPLICADO"), reasons);

        var first = MAPPER.readTree(aggregates.readValue());
        assertEquals(BASE.toString(), first.get("window_start").asText());
        assertEquals(3, first.get("sales_count").intValue());
        assertEquals(7500, first.get("total_clp").intValue());
        assertEquals(2500, first.get("avg_ticket_clp").intValue());
        var second = MAPPER.readTree(aggregates.readValue());
        assertEquals(BASE.plusSeconds(60).toString(), second.get("window_start").asText());
        assertEquals(2, second.get("sales_count").intValue());
        assertEquals(12000, second.get("total_clp").intValue());
        assertEquals(6000, second.get("avg_ticket_clp").intValue());
        assertTrue(aggregates.isEmpty());

        assertEquals(9000, MAPPER.readTree(late.readValue()).get("sale").get("totalClp").intValue());
        assertTrue(late.isEmpty());
    }

    private Sale sale(String eventId, long seconds, int unitPrice) {
        return new Sale(eventId, BASE.plusSeconds(seconds).toString(), "Tienda Prueba", "tienda", "Santiago",
                "presencial", "ropa", "Polera", 2, unitPrice, 2 * unitPrice, CUSTOMER, "debito");
    }
}
