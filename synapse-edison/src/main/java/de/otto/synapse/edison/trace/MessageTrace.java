package de.otto.synapse.edison.trace;

import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableList;
import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.endpoint.MessageEndpoint;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.copyOf;

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
    }

    private final Queue<TraceEntry> traceEntries;
    private final ImmutableList<RegisteredEndpoints> registeredEndpoints;

    /**
     * Creates a new instance with specified capacity.
     *
     * @param capacity the size of the underlying ring buffer.
     */
    public MessageTrace(final int capacity, final Iterable<MessageEndpoint> endpoints) {
        traceEntries = EvictingQueue.create(capacity);
        final ImmutableList.Builder<RegisteredEndpoints> builder = ImmutableList.builder();
        endpoints.forEach(messageEndpoint -> {
            builder.add(new RegisteredEndpoints(messageEndpoint));
            messageEndpoint.getInterceptorChain().register(message -> {
                add(new TraceEntry(messageEndpoint.getChannelName(), messageEndpoint.getEndpointType(), message));
                return message;
            });
        });
        registeredEndpoints = builder.build();
    }

    public List<String> getSenderChannels() {
        return registeredEndpoints
                .stream()
                .filter(registeredEndpoint -> registeredEndpoint.getEndpointType().equals(EndpointType.SENDER))
                .map(RegisteredEndpoints::getChannelName)
                .collect(Collectors.toList());
    }

    public List<String> getReceiverChannels() {
        return registeredEndpoints
                .stream()
                .filter(registeredEndpoint -> registeredEndpoint.getEndpointType().equals(EndpointType.RECEIVER))
                .map(RegisteredEndpoints::getChannelName)
                .collect(Collectors.toList());
    }

    /**
     * Adds a Message to the MessageStore.
     *
     * <p>If the capacity of the ring buffer is reached, the oldest message is removed</p>
     * @param traceEntry the message to add
     */
    public synchronized void add(final TraceEntry traceEntry) {
        traceEntries.add(traceEntry);
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
