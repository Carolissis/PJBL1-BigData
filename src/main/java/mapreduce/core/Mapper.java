package mapreduce.core;
import java.io.IOException;

public abstract class Mapper<K extends Writable, V extends Writable> {
    public abstract void map(long lineNum, String line, Context<K, V> context)
            throws IOException, InterruptedException;
}
