package mapreduce.writables;
import mapreduce.core.WritableComparable;

public class MinMaxAmountWritable implements WritableComparable<MinMaxAmountWritable> {
    private double minAmount;
    private double maxAmount;

    public MinMaxAmountWritable() {
        this.minAmount = Double.MAX_VALUE;
        this.maxAmount = -Double.MAX_VALUE;
    }

    public MinMaxAmountWritable(double amount) {
        this.minAmount = amount;
        this.maxAmount = amount;
    }

    public double getMinAmount() { return minAmount; }
    public double getMaxAmount() { return maxAmount; }

    public void merge(MinMaxAmountWritable other) {
        if (other.minAmount < this.minAmount) this.minAmount = other.minAmount;
        if (other.maxAmount > this.maxAmount) this.maxAmount = other.maxAmount;
    }

    @Override
    public int compareTo(MinMaxAmountWritable o) {
        int cmp = Double.compare(this.maxAmount, o.maxAmount);
        if (cmp != 0) return cmp;
        return Double.compare(this.minAmount, o.minAmount);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MinMaxAmountWritable)) return false;
        MinMaxAmountWritable o = (MinMaxAmountWritable) obj;
        return Double.compare(minAmount, o.minAmount) == 0 && Double.compare(maxAmount, o.maxAmount) == 0;
    }

    @Override
    public int hashCode() {
        return Double.hashCode(minAmount) * 31 + Double.hashCode(maxAmount);
    }

    @Override
    public String toString() {
        return String.format(java.util.Locale.US, "Minimo: %.2f | Maximo: %.2f", minAmount, maxAmount);
    }
}
