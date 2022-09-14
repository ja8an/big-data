package dev.jean.tde1;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.apache.hadoop.io.Text;

import java.util.Objects;


@Getter
@AllArgsConstructor
@ToString
public class Transaction {
    private final String country;
    private final int year;
    private final String commodityCode;
    private final String commodity;
    private final String flow;
    private final Double price;
    private final Double weight;
    private final String unit;
    private final Double amount;
    private final String category;

    public Transaction(String line) {
        String[] temp = line.split(";");
        this.country = temp[0];
        this.year = Integer.parseInt(temp[1]);
        this.commodityCode = temp[2];
        this.commodity = temp[3];
        this.flow = temp[4];
        this.price = Double.parseDouble(assureNotEmpty(temp[5]));
        this.weight = Double.parseDouble(assureNotEmpty(temp[6]));
        this.unit = temp[7];
        this.amount = Double.parseDouble(assureNotEmpty(temp[8]));
        this.category = temp[9];
    }

    private String assureNotEmpty(String input) {
        return Objects.requireNonNullElse(input, "")
                .trim()
                .length() > 0 ? input : "0";
    }

    public static Transaction from(Text text) {
        return new Transaction(text.toString());
    }
}
