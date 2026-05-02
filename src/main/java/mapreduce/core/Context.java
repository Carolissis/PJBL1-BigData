package mapreduce.core;
import java.util.ArrayList;
import java.util.List;

public class Context<K, V> {
    private final List<Pair<K, V>> output = new ArrayList<>();

    public void write(K key, V value) {
        output.add(new Pair<>(key, value));
    }

    public List<Pair<K, V>> getOutput() {
        return output;
    }
}
