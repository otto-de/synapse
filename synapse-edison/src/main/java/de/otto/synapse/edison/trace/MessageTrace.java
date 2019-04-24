package de.otto.synapse.edison.trace;

import com.google.common.collect.EvictingQueue;
import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.endpoint.MessageEndpoint;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Objects;
import java.util.Queue;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Sets.newTreeSet;
import static java.lang.Boolean.TRUE;

/**
 * Thread-safe in-memory implementation of a circular MessageStore that is storing all traceEntries in insertion order
 * with a configurable capacity.
 *
 * <p>Each time an element is added to a full message store, the message store automatically removes its head element.
 */
@ThreadSafe
public class MessageTrace {

    private static final class RegisteredEndpoints {
        private final String channelName;
        private final EndpointType endpointType;

        RegisteredEndpoints(final MessageEndpoint messageEndpoint) {
            this.channelName = messageEndpoint.getChannelName();
            this.endpointType = messageEndpoint.getEndpointType();
        }

        String getChannelName() {
            return channelName;
        }

        EndpointType getEndpointType() {
            return endpointType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RegisteredEndpoints that = (RegisteredEndpoints) o;
            return Objects.equals(channelName, that.channelName) &&
                    endpointType == that.endpointType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(channelName, endpointType);
        }

        @Override
        public String toString() {
            return "RegisteredEndpoints{" +
                    "channelName='" + channelName + '\'' +
                    ", endpointType=" + endpointType +
                    '}';
        }
    }

    private final Queue<TraceEntry> traceEntries;
    private final int capacity;
    private final ConcurrentMap<String, Boolean> senders = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Boolean> receivers = new ConcurrentHashMap<>();

    /**
     * Creates a new instance with specified capacity.
     *
     * @param capacity the size of the underlying ring buffer.
     */
    public MessageTrace(final int capacity) {
        traceEntries = EvictingQueue.create(capacity);
        this.capacity = capacity;
    }

    public SortedSet<String> getSenderChannels() {
        return newTreeSet(senders.keySet());
    }

    public SortedSet<String> getReceiverChannels() {
        return newTreeSet(receivers.keySet());
    }

    public int getCapacity() {
        return capacity;
    }

    /**
     * Adds a Message to the MessageStore.
     *
     * <p>If the capacity of the ring buffer is reached, the oldest message is removed</p>
     * @param traceEntry the message to add
     */
    public synchronized void add(final TraceEntry traceEntry) {
        traceEntries.add(traceEntry);
        if (traceEntry.getEndpointType() == EndpointType.RECEIVER) {
            receivers.putIfAbsent(traceEntry.getChannelName(), TRUE);
        } else if (traceEntry.getEndpointType() == EndpointType.SENDER) {
            senders.putIfAbsent(traceEntry.getChannelName(), TRUE);
        }
    }

    /**
     * Returns a Stream of {@link TraceEntry traceEntries} contained in the RegisteredEndpoints.
     * <p>
     *     The stream will maintain the insertion order of the traceEntries.
     * </p>
     *
     * @return Stream of traceEntries
     */
    public synchronized Stream<TraceEntry> stream() {
        return copyOf(traceEntries).stream();
    }

    public synchronized Stream<TraceEntry> stream(final String channelName, final EndpointType endpointType) {
        return copyOf(traceEntries)
                .stream()
                .filter(traceEntry -> traceEntry.getChannelName().equals(channelName) && traceEntry.getEndpointType().equals(endpointType));
    }

}
