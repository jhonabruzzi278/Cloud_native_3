package cl.duoc.ventas;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Secuencia controlada de la EP5. Con ventana de 60 s y gracia de 30 s, sobre una base T alineada al minuto:
 *
 * <pre>
 *  #  caso                     eventTime  efecto esperado
 *  1  VALIDA_1                 T+5s       ventana [T, T+60) · 2.000
 *  2  JSON_MALFORMADO          -          DLQ
 *  3  CAMPO_AUSENTE            T+5s       DLQ (falta store)
 *  4  TOTAL_INCORRECTO         T+5s       DLQ
 *  5  EVENT_TIME_INVALIDO      "ayer"     DLQ
 *  6  VALIDA_2                 T+20s      ventana [T, T+60) · 2.500
 *  7  DUPLICADO                T+20s      DLQ EVENTO_DUPLICADO (mismo eventId que 6)
 *  8  AVANCE_1                 T+70s      ventana [T+60, T+120) · 5.000; stream time T+70
 *  9  TARDIO_DENTRO_DE_GRACIA  T+30s      fuera de orden, la ventana cierra en T+90 > T+70: se suma · 3.000
 * 10  AVANCE_2                 T+100s     ventana [T+60, T+120) · 7.000; cierra [T, T+60)
 * 11  TARDIO_FUERA_DE_GRACIA   T+40s      T+90 <= T+100: va a ventas-tardias, no se suma · 9.000
 * 12  CIERRE                   T+200s     cierra [T+60, T+120)
 * </pre>
 *
 * Resultado: [T, T+60) = 3 ventas, 7.500 CLP, ticket 2.500; [T+60, T+120) = 2 ventas, 12.000 CLP, ticket 6.000.
 */
final class Scenarios {
    static final String CUSTOMER = "cliente-escenarios";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    record Scenario(String label, String key, String value) { }

    private Scenarios() {
    }

    static List<Scenario> build(Instant base, String store) {
        String run = Long.toString(base.getEpochSecond());
        var list = new ArrayList<Scenario>();
        Sale first = sale("esc-" + run + "-1", base.plusSeconds(5), store, 2, 1000);
        list.add(valid("VALIDA_1", first));
        list.add(new Scenario("JSON_MALFORMADO", CUSTOMER, "{\"eventId\":"));
        ObjectNode missing = MAPPER.valueToTree(sale("esc-" + run + "-3", base.plusSeconds(5), store, 2, 1000));
        missing.remove("store");
        list.add(new Scenario("CAMPO_AUSENTE", CUSTOMER, missing.toString()));
        ObjectNode wrong = MAPPER.valueToTree(sale("esc-" + run + "-4", base.plusSeconds(5), store, 2, 1000));
        wrong.put("totalClp", 1234);
        list.add(new Scenario("TOTAL_INCORRECTO", CUSTOMER, wrong.toString()));
        ObjectNode badTime = MAPPER.valueToTree(sale("esc-" + run + "-5", base.plusSeconds(5), store, 2, 1000));
        badTime.put("eventTime", "ayer");
        list.add(new Scenario("EVENT_TIME_INVALIDO", CUSTOMER, badTime.toString()));
        Sale second = sale("esc-" + run + "-6", base.plusSeconds(20), store, 2, 1250);
        list.add(valid("VALIDA_2", second));
        list.add(valid("DUPLICADO", second));
        list.add(valid("AVANCE_1", sale("esc-" + run + "-8", base.plusSeconds(70), store, 1, 5000)));
        list.add(valid("TARDIO_DENTRO_DE_GRACIA", sale("esc-" + run + "-9", base.plusSeconds(30), store, 1, 3000)));
        list.add(valid("AVANCE_2", sale("esc-" + run + "-10", base.plusSeconds(100), store, 1, 7000)));
        list.add(valid("TARDIO_FUERA_DE_GRACIA", sale("esc-" + run + "-11", base.plusSeconds(40), store, 1, 9000)));
        list.add(valid("CIERRE", sale("esc-" + run + "-12", base.plusSeconds(200), store, 1, 1000)));
        return list;
    }

    static Sale sale(String eventId, Instant eventTime, String store, int quantity, int unitPrice) {
        return new Sale(eventId, eventTime.toString(), store, "tienda", "Santiago", "presencial", "ropa",
                "Polera", quantity, unitPrice, quantity * unitPrice, CUSTOMER, "debito");
    }

    private static Scenario valid(String label, Sale sale) {
        try {
            return new Scenario(label, sale.customerId(), MAPPER.writeValueAsString(sale));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
