package dev.jean.tde1;

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;

import java.util.Objects;

@Getter
@ToString
public class Transaction {

    private final String country;
    private final int year;
    private final String commodityCode;
    private final String commodity;
    private final String flow;
    private final double price;
    private final double weight;
    private final String unit;
    private final double amount;
    private final String category;

    @SneakyThrows
    public Transaction(final String line) {
        String[] columns = line.split(";");
        this.country = columns[0];
        this.year = Integer.parseInt(columns[1]);
        this.commodityCode = columns[2];
        this.commodity = columns[3];
        this.flow = columns[4];
        this.price = Double.parseDouble(columns[5]);
        this.weight = Double.parseDouble(assureNotEmpty(columns[6]));
        this.unit = columns[7];
        this.amount = Double.parseDouble(columns[8]);
        this.category = columns[9];
    }

    private String assureNotEmpty(final String input) {
        return Objects.requireNonNullElse(input, "").trim().length() > 0 ? input : "0";
    }
}
