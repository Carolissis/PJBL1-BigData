package mapreduce.writables;

import mapreduce.core.Writable;

/**
 * Writable customizado que armazena o preço mínimo e máximo de um grupo.
 * Usado em Q6 com Combiner obrigatório.
 */
public class MinMaxPriceWritable implements Writable {
    private double minPrice;
    private double maxPrice;

    public MinMaxPriceWritable() {
        this.minPrice = Double.MAX_VALUE;
        this.maxPrice = Double.MIN_VALUE;
    }

    public MinMaxPriceWritable(double price) {
        this.minPrice = price;
        this.maxPrice = price;
    }

    public double getMinPrice() { return minPrice; }
    public double getMaxPrice() { return maxPrice; }

    public void merge(MinMaxPriceWritable other) {
        if (other.minPrice < this.minPrice) this.minPrice = other.minPrice;
        if (other.maxPrice > this.maxPrice) this.maxPrice = other.maxPrice;
    }

    @Override
    public String toString() {
        return String.format(java.util.Locale.US, "Minimo: %.2f | Maximo: %.2f", minPrice, maxPrice);
    }
}
