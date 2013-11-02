public class Pair<X extends Comparable<X>, Y extends Comparable<Y>> implements Comparable<Pair<X, Y>> {

    private final X x;
    private final Y y;

    public Pair(X x, Y y) {
        this.x = x;
        this.y = y;
    }

    public X getX() {
        return x;
    }

    public Y getY() {
        return y;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Pair)) return false;

        Pair pair = (Pair) o;

        if (x != null ? !x.equals(pair.x) : pair.x != null) return false;
        if (y != null ? !y.equals(pair.y) : pair.y != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = x != null ? x.hashCode() : 0;
        result = 31 * result + (y != null ? y.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(Pair<X, Y> o) {
        if (x.compareTo(o.x) == 0) {
            return y.compareTo(o.y);
        } else {
            return x.compareTo(o.x);
        }
    }
}

