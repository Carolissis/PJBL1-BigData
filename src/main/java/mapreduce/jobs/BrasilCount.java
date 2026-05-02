package mapreduce.jobs;
import mapreduce.core.*;
import mapreduce.types.*;
import java.io.IOException;
import java.util.List;

public class BrasilCount {

    public static class MapQ1 extends Mapper<Text, IntWritable> {
        @Override
        public void map(long lineNum, String line, Context<Text, IntWritable> ctx)
                throws IOException, InterruptedException {
            String[] cols = line.split(";", -1);
            if (cols.length < 10) return;
            String country = cols[0].trim();
            if (country.isEmpty()) return;
            if (country.equalsIgnoreCase("Brazil")) {
                ctx.write(new Text("Brazil"), new IntWritable(1));
            }
        }
    }

    public static class ReduceQ1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, List<IntWritable> values, Context<Text, IntWritable> ctx)
                throws IOException, InterruptedException {
            int total = 0;
            for (IntWritable v : values) total += v.get();
            ctx.write(key, new IntWritable(total));
        }
    }

    public static Job buildJob(String input, String output) {
        return Job.create("Q1 - Transacoes do Brasil")
                .setMapper(MapQ1.class)
                .setReducer(ReduceQ1.class)
                .setInput(input)
                .setOutput(output);
    }
}
