package dev.jean.tde1;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Activity6 extends BaseTDE<Text, DoubleWritable, Text, Activity6.AVG> {

    public static void main(final String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.exit((new Activity6()).run(true) ? 0 : 1);
    }

    public Activity6() {
        super(Text.class, DoubleWritable.class, Text.class, AVG.class);
    }

    @Override
    protected void map(final LongWritable longWritable, final Text text,
                       final Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        if (longWritable.get() == 0) return;
        Transaction transaction = new Transaction(text.toString());
        String key = String.format("%s.%s", transaction.getUnit(), transaction.getYear());
        context.write(new Text(key), new DoubleWritable(transaction.getPrice()));
    }

    @Override
    protected void reduce(final Text text, final Iterable<DoubleWritable> values,
                          final Reducer<Text, DoubleWritable, Text, AVG>.Context context) throws IOException, InterruptedException {
        double min = -1;
        double max = 0;
        double sum = 0;
        double count = 0;

        for (DoubleWritable value : values) {
            double val = value.get();
            if (min == -1) min = val;
            min = Double.min(min, val);
            max = Double.max(max, val);
            sum += val;
            count++;
        }

        if (max < min) {
            throw new RuntimeException("FEZ ERRADO, SUA ANTA");
        }

        context.write(text, new AVG(min, max, sum / count));
    }

    @Getter
    @Setter
    @ToString
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AVG implements Writable {

        private double min;
        private double max;
        private double med;

        @Override
        public void write(final DataOutput out) throws IOException {
            out.writeDouble(min);
            out.writeDouble(max);
            out.writeDouble(med);
        }

        @Override
        public void readFields(final DataInput in) throws IOException {
            this.min = in.readDouble();
            this.max = in.readDouble();
            this.med = in.readDouble();
        }
    }

}
