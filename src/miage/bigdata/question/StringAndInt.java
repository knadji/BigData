package miage.bigdata.question;
public class StringAndInt implements Comparable<StringAndInt> {

    private String tag;
    private double nbOccurTag;

    @Override
    public int compareTo(final StringAndInt o) {
        if (this.nbOccurTag > o.nbOccurTag) {
            return 1;
        } else if (this.nbOccurTag < o.nbOccurTag) {
            return -1;
        }
        return 0;
    }

    @Override
    public String toString() {
        return "[tag=" + tag + ", nbOccurTag=" + nbOccurTag + "]";
    }

}
