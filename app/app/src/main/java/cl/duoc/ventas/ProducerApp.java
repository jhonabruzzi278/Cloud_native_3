package cl.duoc.ventas;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ProducerApp {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private record Store(String name, String format, String city) {
    }

    private record Product(String name, String category, int minPrice, int maxPrice) {
    }

    private static final List<Store> STORES = List.of(
            new Store("Paris Costanera Center", "tienda", "Santiago"),
            new Store("Paris Plaza Vespucio", "tienda", "Santiago"),
            new Store("Paris Marina Arauco", "tienda", "Viña del Mar"),
            new Store("Paris Online", "online", "Santiago"),
            new Store("Falabella Costanera Center", "tienda", "Santiago"),
            new Store("Falabella Plaza Oeste", "tienda", "Santiago"),
            new Store("Falabella Online", "online", "Santiago"));

    private static final List<Product> PRODUCTS = List.of(
            new Product("Polera algodón", "ropa", 9990, 24990),
            new Product("Jeans slim", "ropa", 19990, 49990),
            new Product("Zapatillas running", "ropa", 29990, 89990),
            new Product("Chaqueta cortaviento", "ropa", 24990, 69990),
            new Product("Arroz 1 kg", "supermercado", 1290, 2190),
            new Product("Leche 1 L", "supermercado", 990, 1590),
            new Product("Café molido 250 g", "supermercado", 3990, 7990),
            new Product("Detergente 3 L", "supermercado", 6990, 12990));

    private static final String[] PAYMENT_METHODS = {"débito", "crédito", "efectivo"};

    public static void main(String[] args) throws Exception {
        String bootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092");
        String topic = env("RAW_TOPIC", "ventas-raw");
        double intervalSeconds = Double.parseDouble(env("PRODUCE_INTERVAL", "1.0"));
        long maxEvents = Long.parseLong(env("PRODUCER_MAX_EVENTS", "0"));
        int customerPool = Integer.parseInt(env("CUSTOMER_POOL", "200"));

        var props = new java.util.Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        var running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        if ("escenarios".equalsIgnoreCase(env("PRODUCER_MODE", "aleatorio"))) {
            try (var producer = new KafkaProducer<String, String>(props)) {
                Instant base = Instant.now().truncatedTo(ChronoUnit.MINUTES).plus(1, ChronoUnit.MINUTES);
                for (var scenario : Scenarios.build(base, env("SCENARIO_STORE", "Tienda Escenarios"))) {
                    var metadata = producer.send(new ProducerRecord<>(topic, scenario.key(), scenario.value())).get();
                    System.out.printf("%-24s particion=%d offset=%d clave=%s valor=%s%n", scenario.label(),
                            metadata.partition(), metadata.offset(), scenario.key(), scenario.value());
                }
                producer.flush();
            }
            return;
        }

        System.out.printf("produciendo ventas en '%s' via %s (intervalo %.2fs, max %d)%n",
                topic, bootstrap, intervalSeconds, maxEvents);

        try (var producer = new KafkaProducer<String, String>(props)) {
            long sent = 0;
            while (running.get()) {
                Sale sale = randomSale(customerPool);
                String json = MAPPER.writeValueAsString(sale);
                producer.send(new ProducerRecord<>(topic, sale.customerId(), json)).get();
                sent++;
                System.out.println(json);
                if (maxEvents > 0 && sent >= maxEvents) {
                    break;
                }
                long jitter = Math.round(intervalSeconds * 1000
                        * ThreadLocalRandom.current().nextDouble(0.7, 1.3));
                Thread.sleep(Math.max(1, jitter));
            }
            producer.flush();
            System.out.printf("producer detenido tras %d eventos%n", sent);
        }
    }

    private static Sale randomSale(int customerPool) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        Store store = STORES.get(random.nextInt(STORES.size()));
        Product product = PRODUCTS.get(random.nextInt(PRODUCTS.size()));
        int quantity = random.nextInt(1, 4);
        int unitPrice = random.nextInt(product.minPrice(), product.maxPrice() + 1);
        int total = unitPrice * quantity;

        return new Sale(
                UUID.randomUUID().toString(),
                Instant.now().toString(),
                store.name(),
                store.format(),
                store.city(),
                store.format().equals("online") ? "web" : "presencial",
                product.category(),
                product.name(),
                quantity,
                unitPrice,
                total,
                String.format("cliente-%04d", random.nextInt(1, customerPool + 1)),
                PAYMENT_METHODS[random.nextInt(PAYMENT_METHODS.length)]);
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }
}
