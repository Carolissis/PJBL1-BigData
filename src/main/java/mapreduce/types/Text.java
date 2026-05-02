package mapreduce.types;
import mapreduce.core.WritableComparable;

public class Text implements WritableComparable<Text> {
    private String value;

    public Text() { this.value = ""; }
    public Text(String value) { this.value = value == null ? "" : value; }

    public String get() { return value; }
    public void set(String value) { this.value = value == null ? "" : value; }

    @Override public int compareTo(Text o) { return this.value.compareTo(o.value); }
    @Override public boolean equals(Object o) { return o instanceof Text && ((Text)o).value.equals(value); }
    @Override public int hashCode() { return value.hashCode(); }
    @Override public String toString() { return value; }
}
