package de.otto.synapse.messagestore;

import java.io.IOException;

public interface DurableMessageStore extends MessageStore {

    public void load() throws IOException;

    public void save() throws IOException;
}
