package de.otto.synapse.eventsource.aws;

import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpoint;
import de.otto.synapse.eventsource.AbstractEventSource;

public class KinesisEventSource extends AbstractEventSource {

    public KinesisEventSource(final String name,
                              final MessageLogReceiverEndpoint messageLog) {
        super(name, messageLog);
    }

}
