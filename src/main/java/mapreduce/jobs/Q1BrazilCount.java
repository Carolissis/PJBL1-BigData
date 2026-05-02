package mapreduce.jobs;

import mapreduce.core.*;
import mapreduce.types.*;

import java.io.IOException;
import java.util.List;

/**
 * Q1 – Número de transações envolvendo o Brasil.
 * Colunas: [0]=country_or_area [1]=year  [2]=comm_code    [3]=commodity [4]=flow
 *          [5]=trade_usd      [6]=weight_kg [7]=quantity_name [8]=quantity [9]=category
 */
public class Q1BrazilCount {

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
