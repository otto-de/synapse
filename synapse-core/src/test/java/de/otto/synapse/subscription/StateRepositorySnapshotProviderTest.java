package de.otto.synapse.subscription;

import com.google.common.collect.ImmutableList;
import de.otto.synapse.message.Message;
import de.otto.synapse.message.TextMessage;
import de.otto.synapse.state.ConcurrentMapStateRepository;
import de.otto.synapse.state.StateRepository;
import org.junit.Test;

import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class StateRepositorySnapshotProviderTest {

    @Test
    public void shouldReturnChannelName() {
        final StateRepositorySnapshotProvider<String> snapshotProvider = new StateRepositorySnapshotProvider<>("foo", mock(StateRepository.class));
        assertThat(snapshotProvider.channelName(), is("foo"));
    }

    @Test
    public void shouldStreamSnapshotMessages() {
        final StateRepository<String> stateRepository = new ConcurrentMapStateRepository<>("foo");
        final StateRepositorySnapshotProvider<String> snapshotProvider = new StateRepositorySnapshotProvider<>("foo", stateRepository);
        stateRepository.put("1", "eins");
        Stream<? extends Message<?>> snapshot = snapshotProvider.snapshot("1");
        assertThat(snapshot.collect(toList()), contains(TextMessage.of("1", "eins")));
    }

    @Test
    public void shouldTransformSnapshotMessages() {
        final StateRepository<String> stateRepository = new ConcurrentMapStateRepository<>("foo");
        final EntityToMessageListTransformer<String> entityToMessagesTransformer = (key, entity) -> ImmutableList.of(
                Message.message(key + "-first", "one"),
                Message.message(key + "-second", "two"));
        final StateRepositorySnapshotProvider<String> snapshotProvider = new StateRepositorySnapshotProvider<>("foo", stateRepository, entityToMessagesTransformer);
        stateRepository.put("1", "eins");

        final Stream<? extends Message<?>> snapshot = snapshotProvider.snapshot("1");
        assertThat(snapshot.collect(toList()), contains(
                TextMessage.of("1-first", "one"),
                TextMessage.of("1-second", "two")));
    }

    @Test
    public void shouldStreamNothingForUnknownEntity() {
        final StateRepository<String> stateRepository = new ConcurrentMapStateRepository<>("foo");
        final StateRepositorySnapshotProvider<String> snapshotProvider = new StateRepositorySnapshotProvider<>("foo", stateRepository);
        final Stream<? extends Message<?>> snapshot = snapshotProvider.snapshot("1");
        assertThat(snapshot.count(), is(0L));
    }

}