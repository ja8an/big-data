package dev.jean.tde1;

import dev.jean.base.SimpleHadoop;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Activity4 extends BaseTDE<Text, DoubleWritable, Text, DoubleWritable> {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.exit((new Activity4()).run(true) ? 0 : 1);
    }

    public Activity4() {
        super(Text.class, DoubleWritable.class, Text.class, DoubleWritable.class);
        clearOutputFolder();
    }

    @Override
    protected void map(LongWritable longWritable, Text text, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
        if (longWritable.get() == 0) return;
        Transaction transaction = new Transaction(text.toString());
        String key = String.format("%s.%s", transaction.getCommodityCode(), transaction.getYear());
        context.write(new Text(key), new DoubleWritable(transaction.getPrice()));
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
