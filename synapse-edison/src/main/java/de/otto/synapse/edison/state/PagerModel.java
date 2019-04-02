package de.otto.synapse.edison.state;

import java.util.Objects;

public class PagerModel {
    public static final PagerModel UNAVAILABLE = new PagerModel("", null, null, null, null);

    public final boolean available;
    public final String first;
    public final String last;
    public final String prev;
    public final String next;

    public PagerModel(final String self,
                       final String first,
                       final String prev,
                       final String next,
                       final String last) {
        this.first = nonSelf(first, self);
        this.prev = nonSelf(prev, self);
        this.next = nonSelf(next, self);
        this.last = nonSelf(last, self);
        this.available = this.first != null || this.last != null || this.prev != null || this.next != null;
    }

    private String nonSelf(final String href, final String self) {
        if (href == null || href.equals(self)) {
            return null;
        } else {
            return href;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PagerModel that = (PagerModel) o;
        return available == that.available &&
                Objects.equals(first, that.first) &&
                Objects.equals(last, that.last) &&
                Objects.equals(prev, that.prev) &&
                Objects.equals(next, that.next);
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, first, last, prev, next);
    }

    @Override
    public String toString() {
        return "PagerModel{" +
                "available=" + available +
                ", first='" + first + '\'' +
                ", last='" + last + '\'' +
                ", prev='" + prev + '\'' +
                ", next='" + next + '\'' +
                '}';
    }
}
