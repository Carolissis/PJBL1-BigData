package mapreduce.jobs;
import mapreduce.core.*;
import mapreduce.types.*;
import java.io.IOException;
import java.util.List;

public class TransactionsByCategory {

    public static class MapQ3 extends Mapper<Text, IntWritable> {
        @Override
        public void map(long lineNum, String line, Context<Text, IntWritable> ctx)
                throws IOException, InterruptedException {
            String[] cols = line.split(";", -1);
            if (cols.length < 10) return;
            String category = cols[9].trim();
            if (category.isEmpty()) return;
            ctx.write(new Text(category), new IntWritable(1));
        }
    }

    public static class ReduceQ3 extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, List<IntWritable> values, Context<Text, IntWritable> ctx)
                throws IOException, InterruptedException {
            int total = 0;
            for (IntWritable v : values) total += v.get();
            ctx.write(key, new IntWritable(total));
        }
    }

    public static Job buildJob(String input, String output) {
        return Job.create("Q3 - Transacoes por Categoria")
                .setMapper(MapQ3.class)
                .setReducer(ReduceQ3.class)
                .setInput(input)
                .setOutput(output);
    }
}
