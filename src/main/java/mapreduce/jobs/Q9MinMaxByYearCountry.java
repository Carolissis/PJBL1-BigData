package mapreduce.jobs;

import mapreduce.core.*;
import mapreduce.writables.MinMaxAmountWritable;
import mapreduce.writables.YearCountryKey;

import java.io.IOException;
import java.util.List;

/**
 * Q9 – Transação com maior e menor Amount, por ano e país.
 * Uso obrigatório de Comparable writable (YearCountryKey) e Combiner.
 * Chave composta via YearCountryKey — sem concatenação de strings.
 */
public class Q9MinMaxByYearCountry {

    public static class MapQ9 extends Mapper<YearCountryKey, MinMaxAmountWritable> {
        @Override
        public void map(long lineNum, String line, Context<YearCountryKey, MinMaxAmountWritable> ctx)
                throws IOException, InterruptedException {
            String[] cols = line.split(";", -1);
            if (cols.length < 10) return;
            String country   = cols[0].trim();
            String yearStr   = cols[1].trim();
            String amountStr = cols[8].trim();
            if (country.isEmpty() || yearStr.isEmpty() || amountStr.isEmpty()) return;
            try {
                int    year   = Integer.parseInt(yearStr);
                double amount = Double.parseDouble(amountStr);
                ctx.write(new YearCountryKey(year, country), new MinMaxAmountWritable(amount));
            } catch (NumberFormatException e) { /* ignora dado inválido */ }
        }
    }

    public static class CombineQ9
            extends Reducer<YearCountryKey, MinMaxAmountWritable, YearCountryKey, MinMaxAmountWritable> {
        @Override
        public void reduce(YearCountryKey key, List<MinMaxAmountWritable> values,
                           Context<YearCountryKey, MinMaxAmountWritable> ctx)
                throws IOException, InterruptedException {
            MinMaxAmountWritable acc = new MinMaxAmountWritable();
            for (MinMaxAmountWritable v : values) acc.merge(v);
            ctx.write(key, acc);
        }
    }

    public static class ReduceQ9
            extends Reducer<YearCountryKey, MinMaxAmountWritable, YearCountryKey, MinMaxAmountWritable> {
        @Override
        public void reduce(YearCountryKey key, List<MinMaxAmountWritable> values,
                           Context<YearCountryKey, MinMaxAmountWritable> ctx)
                throws IOException, InterruptedException {
            MinMaxAmountWritable acc = new MinMaxAmountWritable();
            for (MinMaxAmountWritable v : values) acc.merge(v);
            ctx.write(key, acc);
        }
    }

    public static Job buildJob(String input, String output) {
        return Job.create("Q9 - Min e Max Amount por Ano e Pais")
                .setMapper(MapQ9.class)
                .setCombiner(CombineQ9.class)
                .setReducer(ReduceQ9.class)
                .setInput(input)
                .setOutput(output);
    }
}
