package dev.jean.tde1;

import dev.jean.BaseJob;
import dev.jean.utils.InputFile;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Activity4 extends BaseJob<LongWritable, Text, Text, DoubleWritable, Text, DoubleWritable> {

    public Activity4() {
        super(InputFile.TRANSACTIONS, MyMapper.class, MyReducer.class);
    }

    @NoArgsConstructor
    public static class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map(LongWritable lineNumber, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
            if (lineNumber.get() == 0) return;
            Transaction transaction = Transaction.from(value);
            String key = String.format("%s.%s",
                    transaction.getCommodityCode(),
                    transaction.getYear());
            context.write(new Text(key), new DoubleWritable(transaction.getPrice()));
        }
    }

    @NoArgsConstructor
    public static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
            int count = 0;
            double sum = 0;
            for (DoubleWritable value : values) {
                count++;
                sum += value.get();
            }
            context.write(key, new DoubleWritable(sum / count));
        }
    }

    public static void main(String[] args) {
        BaseJob.debug(Activity4.class);
    }

}
