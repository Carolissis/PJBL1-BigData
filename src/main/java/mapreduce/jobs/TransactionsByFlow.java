package mapreduce.jobs;
import mapreduce.core.*;
import mapreduce.types.*;
import java.io.IOException;
import java.util.List;

public class TransactionsByFlow {

    public static class MapQ4 extends Mapper<Text, IntWritable> {
        @Override
        public void map(long lineNum, String line, Context<Text, IntWritable> ctx)
                throws IOException, InterruptedException {
            String[] cols = line.split(";", -1);
            if (cols.length < 10) return;
            String flow = cols[4].trim();
            if (flow.isEmpty()) return;
            ctx.write(new Text(flow), new IntWritable(1));
        }
    }

    public static class ReduceQ4 extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, List<IntWritable> values, Context<Text, IntWritable> ctx)
                throws IOException, InterruptedException {
            int total = 0;
            for (IntWritable v : values) total += v.get();
            ctx.write(key, new IntWritable(total));
        }
    }

    public static Job buildJob(String input, String output) {
        return Job.create("Q4 - Transacoes por Fluxo")
                .setMapper(MapQ4.class)
                .setReducer(ReduceQ4.class)
                .setInput(input)
                .setOutput(output);
    }
}
