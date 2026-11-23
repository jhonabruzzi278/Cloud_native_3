package cl.duoc.ventas;

public record Sale(
        String eventId,
        String eventTime,
        String store,
        String storeFormat,
        String city,
        String channel,
        String category,
        String product,
        int quantity,
        int unitPriceClp,
        int totalClp,
        String customerId,
        String paymentMethod) {
}
