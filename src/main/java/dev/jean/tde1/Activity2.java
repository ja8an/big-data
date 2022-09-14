package dev.jean.tde1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Activity2 extends BaseTDE<IntWritable, IntWritable, IntWritable, IntWritable> {

    public static void main(final String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.exit((new Activity2()).run(true) ? 0 : 1);
    }

    public Activity2() {
        super(IntWritable.class, IntWritable.class, IntWritable.class, IntWritable.class);
    }

    @Override
    public void map(final LongWritable longWritable, final Text text,
                    final Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
        if (longWritable.get() == 0) return;
        Transaction transaction = new Transaction(text.toString());
        context.write(new IntWritable(transaction.getYear()), new IntWritable(1));
    }

    @Override
    public void reduce(final IntWritable intWritable, final Iterable<IntWritable> values,
                       final Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(intWritable, new IntWritable(sum));
    }
}
