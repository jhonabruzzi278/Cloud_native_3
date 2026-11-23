package cl.duoc.ventas;

public class TicketAggregate {

    public long count;
    public long totalClp;

    public TicketAggregate() {
    }

    public TicketAggregate add(Sale sale) {
        count++;
        totalClp += sale.totalClp();
        return this;
    }
}
