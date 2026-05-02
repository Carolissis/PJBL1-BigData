package mapreduce.writables;
import mapreduce.core.WritableComparable;

public class YearCountryKey implements WritableComparable<YearCountryKey> {
    private int    year;
    private String country;

    public YearCountryKey() {}
    public YearCountryKey(int year, String country) {
        this.year    = year;
        this.country = country;
    }

    public int    getYear()    { return year; }
    public String getCountry() { return country; }

    @Override
    public int compareTo(YearCountryKey o) {
        int cmp = Integer.compare(this.year, o.year);
        if (cmp != 0) return cmp;
        return this.country.compareTo(o.country);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof YearCountryKey)) return false;
        YearCountryKey o = (YearCountryKey) obj;
        return this.year == o.year && this.country.equals(o.country);
    }

    @Override
    public int hashCode() {
        return 31 * Integer.hashCode(year) + country.hashCode();
    }

    @Override
    public String toString() {
        return year + " | " + country;
    }
}
