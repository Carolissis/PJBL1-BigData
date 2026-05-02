package mapreduce.core;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Orchestrates the MapReduce pipeline: Map → (Combiner) → Shuffle+Sort → Reduce → Output.
 * Implementação do zero, sem uso de biblioteca Hadoop.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class Job {

    private String name;
    private String inputPath;
    private String outputPath;
    private Class<? extends Mapper>  mapperClass;
    private Class<? extends Reducer> reducerClass;
    private Class<? extends Reducer> combinerClass;
    private boolean skipHeader = true;

    private Job() {}

    public static Job create(String name) {
        Job j = new Job();
        j.name = name;
        return j;
    }

    public Job setMapper(Class<? extends Mapper> c)  { this.mapperClass  = c; return this; }
    public Job setReducer(Class<? extends Reducer> c) { this.reducerClass = c; return this; }
    public Job setCombiner(Class<? extends Reducer> c){ this.combinerClass= c; return this; }
    public Job setInput(String path)  { this.inputPath  = path; return this; }
    public Job setOutput(String path) { this.outputPath = path; return this; }
    public Job setSkipHeader(boolean skip) { this.skipHeader = skip; return this; }
    public String getOutputPath() { return outputPath; }

    public List<String> waitForCompletion(boolean verbose) throws Exception {
        if (verbose) System.out.println("\n[" + name + "] Iniciando...");

        // ── MAP ─────────────────────────────────────────────────────────────
        Mapper mapper = mapperClass.getDeclaredConstructor().newInstance();
        List<Pair> mapOutput = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(inputPath), StandardCharsets.UTF_8))) {
            String line;
            long lineNum = 0;
            boolean first = true;
            while ((line = br.readLine()) != null) {
                if (first && skipHeader) { first = false; continue; }
                first = false;
                if (line.trim().isEmpty()) continue;
                Context ctx = new Context();
                mapper.map(lineNum, line, ctx);
                mapOutput.addAll(ctx.getOutput());
                lineNum++;
            }
        }
        if (verbose) System.out.println("[" + name + "] Map: " + mapOutput.size() + " pares gerados.");

        // ── COMBINER (opcional) ──────────────────────────────────────────────
        if (combinerClass != null) {
            Reducer combiner = combinerClass.getDeclaredConstructor().newInstance();
            mapOutput = applyReducer(combiner, mapOutput);
            if (verbose) System.out.println("[" + name + "] Combiner: " + mapOutput.size() + " pares após combiner.");
        }

        // ── SHUFFLE & SORT ───────────────────────────────────────────────────
        sortPairs(mapOutput);
        if (verbose) System.out.println("[" + name + "] Shuffle & Sort concluído.");

        // ── REDUCE ───────────────────────────────────────────────────────────
        Reducer reducer = reducerClass.getDeclaredConstructor().newInstance();
        List<Pair> reduceOutput = applyReducer(reducer, mapOutput);
        if (verbose) System.out.println("[" + name + "] Reduce: " + reduceOutput.size() + " pares na saída.");

        // ── WRITE OUTPUT ─────────────────────────────────────────────────────
        new File(outputPath).getParentFile().mkdirs();
        List<String> lines = new ArrayList<>();
        try (PrintWriter pw = new PrintWriter(
                new OutputStreamWriter(new FileOutputStream(outputPath), StandardCharsets.UTF_8))) {
            for (Pair p : reduceOutput) {
                String out = p.getKey() + "\t" + p.getValue();
                pw.println(out);
                lines.add(out);
            }
        }
        if (verbose) System.out.println("[" + name + "] Resultado salvo em: " + outputPath);
        return lines;
    }

    // ── Helpers ─────────────────────────────────────────────────────────────

    private List<Pair> applyReducer(Reducer reducer, List<Pair> input) throws Exception {
        if (input.isEmpty()) return new ArrayList<>();
        sortPairs(input);

        List<Pair> output = new ArrayList<>();
        int i = 0;
        while (i < input.size()) {
            Object currentKey = input.get(i).getKey();
            List values = new ArrayList();
            while (i < input.size() && compareKeys(input.get(i).getKey(), currentKey) == 0) {
                values.add(input.get(i).getValue());
                i++;
            }
            Context ctx = new Context();
            reducer.reduce((Writable) currentKey, values, ctx);
            output.addAll(ctx.getOutput());
        }
        return output;
    }

    private void sortPairs(List<Pair> pairs) {
        pairs.sort((a, b) -> compareKeys(a.getKey(), b.getKey()));
    }

    private int compareKeys(Object a, Object b) {
        if (a instanceof Comparable) {
            return ((Comparable) a).compareTo(b);
        }
        return a.toString().compareTo(b.toString());
    }
}
