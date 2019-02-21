package de.otto.synapse.state;

import com.google.common.collect.ImmutableSet;
import de.otto.synapse.consumer.MessageConsumer;
import de.otto.synapse.message.Message;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;

public class MessageConsumingStateRepository<S, P> implements StateRepository<S>, MessageConsumer<P> {

    private final Class<P> messagePayloadType;
    private final Pattern keyPattern;
    private final StateRepository<S> delegate;
    private final Function<Message<P>, String> keyMapper;
    private final BiFunction<Optional<S>, Message<P>, S> stateMapper;

    public MessageConsumingStateRepository(final Class<P> messagePayloadType,
                                           final String keyPattern,
                                           final StateRepository<S> backingStateRepository,
                                           final Function<Message<P>, String> keyMapper,
                                           final BiFunction<Optional<S>, Message<P>, S> stateRemappingFunction) {
        this.messagePayloadType = messagePayloadType;
        this.keyPattern = Pattern.compile(keyPattern);
        this.delegate = backingStateRepository;
        this.keyMapper = keyMapper;
        this.stateMapper = stateRemappingFunction;
    }

    public static <S, P> MessageConsumingStateRepository<S, P> of(final Class<P> messagePayloadType,
                                                                  final StateRepository<S> backingStateRepository,
                                                                  final BiFunction<Optional<S>, Message<P>, S> stateRemappingFunction) {
        return new MessageConsumingStateRepository<>(messagePayloadType, ".*", backingStateRepository, (m) -> m.getKey().partitionKey(), stateRemappingFunction);
    }

    @Nonnull
    @Override
    public Class<P> payloadType() {
        return messagePayloadType;
    }

    @Nonnull
    @Override
    public Pattern keyPattern() {
        return keyPattern;
    }

    @Override
    public void accept(final Message<P> message) {
        compute(keyMapper.apply(message), (_key, previousValue) -> stateMapper.apply(previousValue, message));
    }

    @Override
    public Optional<S> compute(String key, BiFunction<? super String, ? super Optional<S>, ? extends S> remappingFunction) {
        return delegate.compute(key, remappingFunction);
    }

    @Override
    public void consumeAll(BiConsumer<? super String, ? super S> consumer) {
        delegate.consumeAll(consumer);
    }

    @Override
    public Optional<S> put(String key, S value) {
        return delegate.put(key, value);
    }

    @Override
    public Optional<S> remove(String key) {
        return delegate.remove(key);
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public Optional<S> get(String key) {
        return delegate.get(key);
    }

    @Override
    @Deprecated
    public ImmutableSet<String> keySet() {
        return delegate.keySet();
    }

    @Override
    public long size() {
        return delegate.size();
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }

}
