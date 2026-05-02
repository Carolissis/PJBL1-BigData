package mapreduce;

import mapreduce.core.Job;
import mapreduce.jobs.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class Main {

    private static final String DEFAULT_INPUT = "data/transactions.csv";
    private static final String OUTPUT_FILE   = "output/resultados.txt";
    private static final String TEMP_DIR      = "output/tmp";

    public static void main(String[] args) throws Exception {
        String input = args.length > 0 ? args[0] : DEFAULT_INPUT;

        if (!new File(input).exists()) {
            System.err.println("Dataset nao encontrado: " + input);
            System.err.println("Uso: java -jar mapreduce-pjbl1.jar [caminho.csv]");
            System.exit(1);
        }

        new File(TEMP_DIR).mkdirs();

        try (PrintWriter out = new PrintWriter(
                new OutputStreamWriter(new FileOutputStream(OUTPUT_FILE), StandardCharsets.UTF_8))) {

            exec(out, "Q1", "Numero de transacoes envolvendo o Brasil",
                    Q1BrazilCount.buildJob(input, tmp("q1")));

            exec(out, "Q2", "Numero de transacoes por ano",
                    Q2TransactionsByYear.buildJob(input, tmp("q2")));

            exec(out, "Q3", "Numero de transacoes por categoria",
                    Q3TransactionsByCategory.buildJob(input, tmp("q3")));

            exec(out, "Q4", "Numero de transacoes por tipo de fluxo",
                    Q4TransactionsByFlow.buildJob(input, tmp("q4")));

            exec(out, "Q5", "Valor medio das transacoes por ano no Brasil [AvgWritable]",
                    Q5AvgValuePerYearBrazil.buildJob(input, tmp("q5")));

            exec(out, "Q6", "Transacao mais cara e mais barata no Brasil em 2016 [Combiner + MinMaxPriceWritable]",
                    Q6MinMaxBrazil2016.buildJob(input, tmp("q6")));

            exec(out, "Q7", "Valor medio das transacoes de exportacao no Brasil por ano [Combiner + AvgWritable]",
                    Q7AvgExportBrazil.buildJob(input, tmp("q7")));

            System.out.println("Executando Q8...");
            List<String> q8 = Q8MaxValueSortedBrazil.runChained(
                    input, tmp("q8_intermediate"), tmp("q8_final"));
            writeSection(out, "Q8",
                    "Valor maximo das transacoes por ano no Brasil - ordem decrescente [Job Chaining + DescendingDoubleKey]",
                    q8);
            System.out.println("[Q8] Concluido");

            exec(out, "Q9", "Transacao com maior e menor amount por ano e pais [YearCountryKey WritableComparable + Combiner]",
                    Q9MinMaxByYearCountry.buildJob(input, tmp("q9")));
        }

        System.out.println();
        System.out.println("Resultado salvo em: " + OUTPUT_FILE);
    }

    private static void exec(PrintWriter out, String label, String title, Job job) throws Exception {
        System.out.println("Executando " + label + "...");
        List<String> results = job.waitForCompletion(false);
        writeSection(out, label, title, results);
        System.out.println("[" + label + "] Concluido");
    }

    private static void writeSection(PrintWriter out, String label, String title, List<String> lines) {
        out.println("============================================================");
        out.println(label + " - " + title);
        out.println("============================================================");
        for (String line : lines) out.println(line);
        out.println();
    }

    private static String tmp(String name) {
        return TEMP_DIR + "/" + name + ".txt";
    }
}
