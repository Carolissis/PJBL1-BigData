package mapreduce.jobs;
import mapreduce.core.*;
import mapreduce.types.*;
import mapreduce.writables.MinMaxPriceWritable;
import java.io.IOException;
import java.util.List;

public class MinMaxBrasil {

    public static class MapQ6 extends Mapper<IntWritable, MinMaxPriceWritable> {
        @Override
        public void map(long lineNum, String line, Context<IntWritable, MinMaxPriceWritable> ctx)
                throws IOException, InterruptedException {
            String[] cols = line.split(";", -1);
            if (cols.length < 10) return;
            String country  = cols[0].trim();
            String yearStr  = cols[1].trim();
            String priceStr = cols[5].trim();
            if (country.isEmpty() || yearStr.isEmpty() || priceStr.isEmpty()) return;
            if (!country.equalsIgnoreCase("Brazil")) return;
            try {
                int year = Integer.parseInt(yearStr);
                if (year != 2016) return;
                double price = Double.parseDouble(priceStr);
                ctx.write(new IntWritable(2016), new MinMaxPriceWritable(price));
            } catch (NumberFormatException e) { /* ignora dado inválido */ }
        }
    }

    public static class CombineQ6 extends Reducer<IntWritable, MinMaxPriceWritable, IntWritable, MinMaxPriceWritable> {
        @Override
        public void reduce(IntWritable key, List<MinMaxPriceWritable> values,
                           Context<IntWritable, MinMaxPriceWritable> ctx)
                throws IOException, InterruptedException {
            MinMaxPriceWritable acc = new MinMaxPriceWritable();
            for (MinMaxPriceWritable v : values) acc.merge(v);
            ctx.write(key, acc);
        }
    }

    public static class ReduceQ6 extends Reducer<IntWritable, MinMaxPriceWritable, IntWritable, MinMaxPriceWritable> {
        @Override
        public void reduce(IntWritable key, List<MinMaxPriceWritable> values,
                           Context<IntWritable, MinMaxPriceWritable> ctx)
                throws IOException, InterruptedException {
            MinMaxPriceWritable acc = new MinMaxPriceWritable();
            for (MinMaxPriceWritable v : values) acc.merge(v);
            ctx.write(key, acc);
        }
    }

    public static Job buildJob(String input, String output) {
        return Job.create("Q6 - Min e Max Price no Brasil em 2016")
                .setMapper(MapQ6.class)
                .setCombiner(CombineQ6.class)
                .setReducer(ReduceQ6.class)
                .setInput(input)
                .setOutput(output);
    }
}
