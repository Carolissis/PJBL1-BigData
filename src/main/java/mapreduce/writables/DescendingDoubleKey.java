package mapreduce.writables;
import mapreduce.core.WritableComparable;

public class DescendingDoubleKey implements WritableComparable<DescendingDoubleKey> {
    private double value;

    public DescendingDoubleKey() {}
    public DescendingDoubleKey(double value) { this.value = value; }

    public double getValue() { return value; }

    @Override
    public int compareTo(DescendingDoubleKey o) {
        return Double.compare(o.value, this.value); 
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof DescendingDoubleKey && Double.compare(((DescendingDoubleKey) obj).value, value) == 0;
    }

    @Override
    public int hashCode() { return Double.hashCode(value); }

    @Override
    public String toString() { return String.format(java.util.Locale.US, "%.2f", value); }
}
