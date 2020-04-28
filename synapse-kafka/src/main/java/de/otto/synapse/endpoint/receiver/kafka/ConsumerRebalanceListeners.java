package de.otto.synapse.endpoint.receiver.kafka;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;

public class ConsumerRebalanceListeners implements ConsumerRebalanceListener {
    private final List<ConsumerRebalanceListener> listeners;

    private ConsumerRebalanceListeners(final ConsumerRebalanceListener... listeners) {
        this.listeners = asList(listeners);
    }

    static ConsumerRebalanceListeners of(final ConsumerRebalanceListener... listeners) {
        return new ConsumerRebalanceListeners(listeners);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        listeners.forEach(listener -> listener.onPartitionsAssigned(partitions));
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        listeners.forEach(listener -> listener.onPartitionsRevoked(partitions));
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        listeners.forEach(listener -> listener.onPartitionsLost(partitions));
    }
}
