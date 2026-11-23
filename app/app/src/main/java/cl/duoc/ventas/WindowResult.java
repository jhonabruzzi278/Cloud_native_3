package cl.duoc.ventas;

import com.fasterxml.jackson.annotation.JsonProperty;

public record WindowResult(
        @JsonProperty("window_start") String windowStart,
        @JsonProperty("window_end") String windowEnd,
        String store,
        @JsonProperty("sales_count") long salesCount,
        @JsonProperty("total_clp") long totalClp,
        @JsonProperty("avg_ticket_clp") long avgTicketClp) {
}
