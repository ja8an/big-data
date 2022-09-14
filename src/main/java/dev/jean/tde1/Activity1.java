package dev.jean.tde1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Activity1 extends BaseTDE<Text, IntWritable, Text, IntWritable> {

    public static void main(final String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.exit((new Activity1()).run(true) ? 0 : 1);
    }

    public Activity1() {
        super(Text.class, IntWritable.class, Text.class, IntWritable.class);
    }

    @Override
    public void map(final LongWritable lineNumber, final Text value, final Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        if (lineNumber.get() == 0) return;
        Transaction transaction = new Transaction(value.toString());
        if ("Brazil".equalsIgnoreCase(transaction.getCountry())) {
            context.write(new Text("count"), new IntWritable(1));
        }
    }

    @Override
    public void reduce(final Text text, final Iterable<IntWritable> values,
                       final Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(text, new IntWritable(sum));
    }

}
