package dev.jean.tde1;

import lombok.Getter;
import lombok.ToString;

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

    public Transaction(String line) {
        String[] temp = line.split(";");
        this.country = temp[0];
        this.year = Integer.parseInt(temp[1]);
        this.commodityCode = temp[2];
        this.commodity = temp[3];
        this.flow = temp[4];
        this.price = Double.parseDouble(temp[5]);
        this.weight = Double.parseDouble(assureNotEmpty(temp[6]));
        this.unit = temp[7];
        this.amount = Double.parseDouble(temp[8]);
        this.category = temp[9];
    }

    private String assureNotEmpty(String input) {
        return input.length() > 0 ? input : "0";
    }
}
