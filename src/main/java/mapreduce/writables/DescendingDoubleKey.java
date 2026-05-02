package mapreduce.writables;

import mapreduce.core.WritableComparable;

/**
 * Chave Double que ordena de forma DECRESCENTE (maior para menor).
 * Usada em Q8 Job 2 para ordenar o valor máximo por ano em ordem decrescente.
 */
public class DescendingDoubleKey implements WritableComparable<DescendingDoubleKey> {
    private double value;

    public DescendingDoubleKey() {}
    public DescendingDoubleKey(double value) { this.value = value; }

    public double getValue() { return value; }

    @Override
    public int compareTo(DescendingDoubleKey o) {
        return Double.compare(o.value, this.value); // invertido → ordem decrescente
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
