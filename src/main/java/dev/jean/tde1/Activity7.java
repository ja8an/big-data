package dev.jean.tde1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Activity7 extends BaseTDE<Text, DoubleWritable, Text, DoubleWritable> {

    public static void main(final String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.exit((new Activity7()).run() ? 0 : 1);
    }

    public Activity7() {
        super(Text.class, DoubleWritable.class, Text.class, DoubleWritable.class);
    }

    @Override
    protected void map(final LongWritable longWritable, final Text text,
                       final Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        if (longWritable.get() == 0) return;
        Transaction transaction = new Transaction(text.toString());
        final int year = 2016;
        if (transaction.getYear() == year) {
            String key = String.format("%s.%s", transaction.getFlow(), transaction.getCommodityCode());
            context.write(new Text(key), new DoubleWritable(transaction.getAmount()));
        }
    }

    @Override
    protected void reduce(final Text text, final Iterable<DoubleWritable> values,
                          final Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        float sum = 0;
        for (DoubleWritable value : values) {
            sum += value.get();
        }
        context.write(text, new DoubleWritable(sum));
    }

}
