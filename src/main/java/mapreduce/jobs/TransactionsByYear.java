package mapreduce.jobs;
import mapreduce.core.*;
import mapreduce.types.*;
import java.io.IOException;
import java.util.List;

public class TransactionsByYear {

    public static class MapQ2 extends Mapper<IntWritable, IntWritable> {
        @Override
        public void map(long lineNum, String line, Context<IntWritable, IntWritable> ctx)
                throws IOException, InterruptedException {
            String[] cols = line.split(";", -1);
            if (cols.length < 10) return;
            String yearStr = cols[1].trim();
            if (yearStr.isEmpty()) return;
            try {
                int year = Integer.parseInt(yearStr);
                ctx.write(new IntWritable(year), new IntWritable(1));
            } catch (NumberFormatException e) { /* ignora dado inválido */ }
        }
    }

    public static class ReduceQ2 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, List<IntWritable> values, Context<IntWritable, IntWritable> ctx)
                throws IOException, InterruptedException {
            int total = 0;
            for (IntWritable v : values) total += v.get();
            ctx.write(key, new IntWritable(total));
        }
    }

    public static Job buildJob(String input, String output) {
        return Job.create("Q2 - Transacoes por Ano")
                .setMapper(MapQ2.class)
                .setReducer(ReduceQ2.class)
                .setInput(input)
                .setOutput(output);
    }
}
