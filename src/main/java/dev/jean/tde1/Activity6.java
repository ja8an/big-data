package dev.jean.tde1;

import dev.jean.BaseJob;
import dev.jean.utils.InputFile;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
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

public class Activity6 extends BaseJob<LongWritable, Text, Text, DoubleWritable, Text, Activity6.MyWritable> {

    public Activity6() {
        super(InputFile.TRANSACTIONS, MyMapper.class, MyReducer.class);
    }

    @NoArgsConstructor
    public static class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map(LongWritable lineNumber, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
            if (lineNumber.get() == 0) return;
            Transaction transaction = Transaction.from(value);
            String key = String.format("%s.%d",
                    transaction.getUnit(),
                    transaction.getYear()
            );
            context.write(new Text(key), new DoubleWritable(transaction.getPrice()));
        }
    }

    @NoArgsConstructor
    public static class MyReducer extends Reducer<Text, DoubleWritable, Text, MyWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, MyWritable>.Context context) throws IOException, InterruptedException {
            double min = -1;
            double max = 0;
            double count = 0;
            double sum = 0;
            for (DoubleWritable value : values) {
                double v = value.get();
                if (min == -1) min = v;
                min = Double.min(min, v);
                max = Double.max(max, v);
                sum += v;
                count++;
            }
            context.write(key, new MyWritable(min, max, sum / count));
        }
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @ToString
    public static class MyWritable implements Writable {
        private double min;
        private double max;
        private double mean;

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeDouble(min);
            out.writeDouble(max);
            out.writeDouble(mean);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.min = in.readDouble();
            this.max = in.readDouble();
            this.mean = in.readDouble();
        }
    }

    public static void main(String[] args) {
        BaseJob.debug(Activity6.class);
    }

}
