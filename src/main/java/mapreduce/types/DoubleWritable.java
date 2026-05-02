package mapreduce.types;

import mapreduce.core.WritableComparable;
import java.util.Locale;

public class DoubleWritable implements WritableComparable<DoubleWritable> {
    private double value;

    public DoubleWritable() {}
    public DoubleWritable(double value) { this.value = value; }

    public double get() { return value; }
    public void set(double value) { this.value = value; }

    @Override public int compareTo(DoubleWritable o) { return Double.compare(this.value, o.value); }
    @Override public boolean equals(Object o) { return o instanceof DoubleWritable && Double.compare(((DoubleWritable)o).value, value) == 0; }
    @Override public int hashCode() { return Double.hashCode(value); }
    @Override public String toString() { return String.format(Locale.US, "%.2f", value); }
}
