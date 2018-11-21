package de.otto.synapse.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.util.ReadResolvable;
import net.openhft.chronicle.hash.serialization.BytesReader;
import net.openhft.chronicle.hash.serialization.BytesWriter;

import java.io.IOException;

import static de.otto.synapse.translator.ObjectMappers.defaultObjectMapper;

public final class ChronicleMapBytesMarshaller<V> implements
        BytesWriter<V>,
        BytesReader<V>,
        ReadResolvable<ChronicleMapBytesMarshaller> {

    private final ObjectMapper objectMapper;
    private final Class<V> clazz;

    public ChronicleMapBytesMarshaller(Class<V> clazz) {
        this.objectMapper = defaultObjectMapper();
        this.clazz = clazz;
    }

    public ChronicleMapBytesMarshaller(ObjectMapper objectMapper,
                                       Class<V> clazz) {
        this.objectMapper = objectMapper;
        this.clazz = clazz;
    }

    @Override
    public V read(Bytes in, V using) {
        try {
            return objectMapper.readValue(in.inputStream(), clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(Bytes out, V toWrite) {
        try {
            objectMapper.writeValue(out.writer(), toWrite);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ChronicleMapBytesMarshaller readResolve() {
        return this;
    }
}
