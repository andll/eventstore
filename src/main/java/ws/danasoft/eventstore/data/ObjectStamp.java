package ws.danasoft.eventstore.data;

import java.util.Date;

public class ObjectStamp implements Comparable<ObjectStamp> {
    private final Date date;
    private final long more;

    public ObjectStamp(Date date, long more) {
        this.date = date;
        this.more = more;
    }

    public Date getDate() {
        return date;
    }

    public long getMore() {
        return more;
    }

    @Override
    public int compareTo(ObjectStamp o) {
        int i = date.compareTo(o.getDate());
        if (i != 0) {
            return i;
        }
        return Long.compare(more, o.more);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ObjectStamp that = (ObjectStamp) o;

        if (more != that.more) return false;
        if (!date.equals(that.date)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = date.hashCode();
        result = 31 * result + (int) (more ^ (more >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s:%d", date, more);
    }
}
