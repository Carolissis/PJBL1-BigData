package mapreduce.types;
import mapreduce.core.WritableComparable;

public class IntWritable implements WritableComparable<IntWritable> {
    private int value;

    public IntWritable() {}
    public IntWritable(int value) { this.value = value; }

    public int get() { return value; }
    public void set(int value) { this.value = value; }

    @Override public int compareTo(IntWritable o) { return Integer.compare(this.value, o.value); }
    @Override public boolean equals(Object o) { return o instanceof IntWritable && ((IntWritable)o).value == value; }
    @Override public int hashCode() { return Integer.hashCode(value); }
    @Override public String toString() { return String.valueOf(value); }
}
