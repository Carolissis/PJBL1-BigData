package mapreduce.writables;
import mapreduce.core.Writable;

public class AvgWritable implements Writable {
    private double sum;
    private long   count;

    public AvgWritable() {}
    public AvgWritable(double sum, long count) {
        this.sum   = sum;
        this.count = count;
    }

    public double getSum()   { return sum; }
    public long   getCount() { return count; }
    public double getAverage() { return count == 0 ? 0 : sum / count; }

    public void merge(AvgWritable other) {
        this.sum   += other.sum;
        this.count += other.count;
    }

    @Override
    public String toString() {
        return String.format(java.util.Locale.US, "%.2f", getAverage());
    }
}
