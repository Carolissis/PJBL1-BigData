package mapreduce.types;
import mapreduce.core.WritableComparable;

public class LongWritable implements WritableComparable<LongWritable> {
    private long value;

    public LongWritable() {}
    public LongWritable(long value) { this.value = value; }

    public long get() { return value; }
    public void set(long value) { this.value = value; }

    @Override public int compareTo(LongWritable o) { return Long.compare(this.value, o.value); }
    @Override public boolean equals(Object o) { return o instanceof LongWritable && ((LongWritable)o).value == value; }
    @Override public int hashCode() { return Long.hashCode(value); }
    @Override public String toString() { return String.valueOf(value); }
}
