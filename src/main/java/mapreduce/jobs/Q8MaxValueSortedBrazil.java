package mapreduce.jobs;

import mapreduce.core.*;
import mapreduce.types.*;
import mapreduce.writables.DescendingDoubleKey;

import java.io.IOException;
import java.util.List;

/**
 * Q8 – Valor máximo das transações por ano no Brasil, ordenado do maior para o menor.
 * Concatenação de Jobs obrigatória:
 *   Job 1 → encontra o preço máximo por ano no Brasil
 *   Job 2 → reordena em ordem decrescente usando DescendingDoubleKey
 */
public class Q8MaxValueSortedBrazil {

    // ══════════════════════════ JOB 1 ══════════════════════════════

    public static class MapQ8J1 extends Mapper<IntWritable, DoubleWritable> {
        @Override
        public void map(long lineNum, String line, Context<IntWritable, DoubleWritable> ctx)
                throws IOException, InterruptedException {
            String[] cols = line.split(";", -1);
            if (cols.length < 10) return;
            String country  = cols[0].trim();
            String yearStr  = cols[1].trim();
            String priceStr = cols[5].trim();
            if (country.isEmpty() || yearStr.isEmpty() || priceStr.isEmpty()) return;
            if (!country.equalsIgnoreCase("Brazil")) return;
            try {
                int    year  = Integer.parseInt(yearStr);
                double price = Double.parseDouble(priceStr);
                ctx.write(new IntWritable(year), new DoubleWritable(price));
            } catch (NumberFormatException e) { /* ignora dado inválido */ }
        }
    }

    public static class ReduceQ8J1 extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
        @Override
        public void reduce(IntWritable key, List<DoubleWritable> values,
                           Context<IntWritable, DoubleWritable> ctx)
                throws IOException, InterruptedException {
            double max = -Double.MAX_VALUE;
            for (DoubleWritable v : values) {
                if (v.get() > max) max = v.get();
            }
            ctx.write(key, new DoubleWritable(max));
        }
    }

    // ══════════════════════════ JOB 2 ══════════════════════════════
    // Entrada: linhas "ano\tprecoMax" vindas do Job 1

    public static class MapQ8J2 extends Mapper<DescendingDoubleKey, IntWritable> {
        @Override
        public void map(long lineNum, String line, Context<DescendingDoubleKey, IntWritable> ctx)
                throws IOException, InterruptedException {
            String[] parts = line.split("\t");
            if (parts.length < 2) return;
            try {
                int    year  = Integer.parseInt(parts[0].trim());
                double price = Double.parseDouble(parts[1].trim());
                ctx.write(new DescendingDoubleKey(price), new IntWritable(year));
            } catch (NumberFormatException e) { /* ignora dado inválido */ }
        }
    }

    public static class ReduceQ8J2 extends Reducer<DescendingDoubleKey, IntWritable, IntWritable, DoubleWritable> {
        @Override
        public void reduce(DescendingDoubleKey key, List<IntWritable> values,
                           Context<IntWritable, DoubleWritable> ctx)
                throws IOException, InterruptedException {
            for (IntWritable year : values) {
                ctx.write(year, new DoubleWritable(key.getValue()));
            }
        }
    }

    // ══════════════════════════ EXECUÇÃO ═══════════════════════════

    public static List<String> runChained(String input, String intermediateOutput, String finalOutput)
            throws Exception {
        Job job1 = Job.create("Q8-Job1 - Max Price por Ano no Brasil")
                .setMapper(MapQ8J1.class)
                .setReducer(ReduceQ8J1.class)
                .setInput(input)
                .setOutput(intermediateOutput);
        job1.waitForCompletion(false);

        Job job2 = Job.create("Q8-Job2 - Ordenacao Decrescente")
                .setMapper(MapQ8J2.class)
                .setReducer(ReduceQ8J2.class)
                .setInput(intermediateOutput)
                .setOutput(finalOutput)
                .setSkipHeader(false);
        return job2.waitForCompletion(false);
    }
}
