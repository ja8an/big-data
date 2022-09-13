package dev.jean.tde1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Activity5 extends BaseTDE<Text, DoubleWritable, Text, DoubleWritable> {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.exit((new Activity5()).run(true) ? 0 : 1);
    }

    public Activity5() {
        super(Text.class, DoubleWritable.class, Text.class, DoubleWritable.class);
        clearOutputFolder();
    }

    @Override
    protected void map(LongWritable longWritable, Text text, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        if (longWritable.get() == 0) return;
        Transaction transaction = new Transaction(text.toString());
        if ("Export".equalsIgnoreCase(transaction.getFlow())) {
            String key = String.format("%s.%d.%s", transaction.getUnit(), transaction.getYear(), transaction.getCategory());
            context.write(new Text(key), new DoubleWritable(transaction.getPrice()));
        }
    }

    @Override
    protected void reduce(Text text, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        int count = 0;
        double sum = 0;
        for (DoubleWritable value : values) {
            count++;
            sum += value.get();
        }
        context.write(text, new DoubleWritable(sum / count));
    }

}
