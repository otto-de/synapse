package de.otto.synapse.endpoint;

import de.otto.synapse.channel.selector.MessageLog;
import de.otto.synapse.channel.selector.MessageQueue;
import de.otto.synapse.channel.selector.Selector;
import de.otto.synapse.endpoint.sender.MessageSenderEndpoint;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.translator.MessageFormat;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SpecificMessageLog implements MessageLog {}

public class BestMatchingFactoryComparatorTest {

    @Test
    public void shouldSortSingleMatchingFactoryFirst() {
        final MessageSenderEndpointFactory first = mock(MessageSenderEndpointFactory.class);
        when(first.selector()).thenAnswer((_x) -> MessageLog.class);
        final MessageSenderEndpointFactory second = mock(MessageSenderEndpointFactory.class);
        when(second.selector()).thenAnswer((_x) -> MessageQueue.class);

        final BestMatchingFactoryComparator comparator = new BestMatchingFactoryComparator(MessageLog.class);
        assertThat(comparator.compare(first, second), is(-1));
        assertThat(comparator.compare(first, first), is(0));
        assertThat(comparator.compare(second, first), is(+1));
    }

    @Test
    public void shouldSortMostSpecificFactoryFirst() {
        final MessageSenderEndpointFactory first = mock(MessageSenderEndpointFactory.class);
        when(first.selector()).thenAnswer((_x) -> MessageLog.class);
        final MessageSenderEndpointFactory second = mock(MessageSenderEndpointFactory.class);
        when(second.selector()).thenAnswer((_x) -> SpecificMessageLog.class);

        final BestMatchingFactoryComparator comparator = new BestMatchingFactoryComparator(SpecificMessageLog.class);
        assertThat(comparator.compare(first, second), is(+1));
        assertThat(comparator.compare(second, first), is(-1));
    }

    @Test
    public void shouldSortFactories() {
        final MessageSenderEndpointFactory first = someFactoryFor(SpecificMessageLog.class);
        final MessageSenderEndpointFactory second = someFactoryFor(MessageLog.class);
        final MessageSenderEndpointFactory third = someFactoryFor(MessageQueue.class);

        final List<MessageSenderEndpointFactory> factories = asList(
                first,
                second,
                third,
                first,
                second,
                third);
        factories.sort(new BestMatchingFactoryComparator(SpecificMessageLog.class));
        assertThat(factories, contains(first, first, second, second, third, third));
    }

    private MessageSenderEndpointFactory someFactoryFor(final Class<? extends Selector> selector) {
        return new MessageSenderEndpointFactory() {

            @Override
            public MessageSenderEndpoint create(@Nonnull String channelName, MessageFormat messageFormat) {
                return null;
            }

            @Override
            public boolean matches(Class<? extends Selector> channelSelector) {
                return selector.isAssignableFrom(channelSelector);
            }

            @Override
            public Class<? extends Selector> selector() {
                return selector;
            }

            @Override
            public String toString() {
                return selector.getSimpleName() + " Factory";
            }
        };
    }
}