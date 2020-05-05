package de.otto.synapse.endpoint;

import de.otto.synapse.channel.selector.Selector;

import java.util.Comparator;

public class BestMatchingFactoryComparator implements Comparator<MessageEndpointFactory> {

    private final Class<? extends Selector> selector;

    public BestMatchingFactoryComparator(Class<? extends Selector> selector) {
        this.selector = selector;
    }

    @Override
    public int compare(final MessageEndpointFactory firstCandidate,
                       final MessageEndpointFactory secondCandidate) {
        if (firstCandidate.selector().equals(secondCandidate.selector())) {
            return 0;
        }
        // First, compare exact matches:
        if (firstCandidate.selector().equals(selector)) {
            return -1;
        }
        if (secondCandidate.selector().equals(selector)) {
            return 1;
        }
        // Second, non-exact matches like, for example someone asks for MessageLog and finds a KafkaMessageLog
        if (firstCandidate.matches(selector)) {
            return -1;
        }
        if (secondCandidate.matches(selector)) {
            return +1;
        }
        // Third, fallback to the other way: KafkaMessageLog is requested, we only have an InMemoryMessageLog:
        return selector.isAssignableFrom(firstCandidate.selector()) ? -1 : +1;
    }

}
