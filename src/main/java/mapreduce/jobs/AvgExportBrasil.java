package mapreduce.jobs;
import mapreduce.core.*;
import mapreduce.types.*;
import mapreduce.writables.AvgWritable;
import java.io.IOException;
import java.util.List;

public class AvgExportBrasil {

    public static class MapQ7 extends Mapper<IntWritable, AvgWritable> {
        @Override
        public void map(long lineNum, String line, Context<IntWritable, AvgWritable> ctx)
                throws IOException, InterruptedException {
            String[] cols = line.split(";", -1);
            if (cols.length < 10) return;
            String country  = cols[0].trim();
            String yearStr  = cols[1].trim();
            String flow     = cols[4].trim();
            String priceStr = cols[5].trim();
            if (country.isEmpty() || yearStr.isEmpty() || flow.isEmpty() || priceStr.isEmpty()) return;
            if (!country.equalsIgnoreCase("Brazil")) return;
            if (!flow.equalsIgnoreCase("Export")) return;
            try {
                int    year  = Integer.parseInt(yearStr);
                double price = Double.parseDouble(priceStr);
                ctx.write(new IntWritable(year), new AvgWritable(price, 1));
            } catch (NumberFormatException e) { }
        }
    }

    public static class CombineQ7 extends Reducer<IntWritable, AvgWritable, IntWritable, AvgWritable> {
        @Override
        public void reduce(IntWritable key, List<AvgWritable> values, Context<IntWritable, AvgWritable> ctx)
                throws IOException, InterruptedException {
            AvgWritable acc = new AvgWritable(0, 0);
            for (AvgWritable v : values) acc.merge(v);
            ctx.write(key, acc);
        }
    }

    public static class ReduceQ7 extends Reducer<IntWritable, AvgWritable, IntWritable, AvgWritable> {
        @Override
        public void reduce(IntWritable key, List<AvgWritable> values, Context<IntWritable, AvgWritable> ctx)
                throws IOException, InterruptedException {
            AvgWritable acc = new AvgWritable(0, 0);
            for (AvgWritable v : values) acc.merge(v);
            ctx.write(key, acc);
        }
    }

    public static Job buildJob(String input, String output) {
        return Job.create("Q7 - Valor Medio Exportacoes Brasil por Ano")
                .setMapper(MapQ7.class)
                .setCombiner(CombineQ7.class)
                .setReducer(ReduceQ7.class)
                .setInput(input)
                .setOutput(output);
    }
}
