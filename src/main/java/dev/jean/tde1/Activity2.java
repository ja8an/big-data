package dev.jean.tde1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Activity2 extends BaseTDE<Text, IntWritable, Text, IntWritable> {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.exit((new Activity2()).run(true) ? 0 : 1);
    }

    public Activity2() {
        super(Text.class, IntWritable.class, Text.class, IntWritable.class);
    }

    @Override
    public void map(LongWritable longWritable, Text text, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        if (longWritable.get() == 0)
            return;
        Transaction transaction = new Transaction(text.toString());
        context.write(new Text(String.valueOf(transaction.getYear())), new IntWritable(1));
    }

    @Override
    public void reduce(Text text, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(text, new IntWritable(sum));
    }
}
