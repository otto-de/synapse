package de.otto.synapse.annotation;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_HYPHEN;

public class BeanNameHelper {

    public static String beanNameForEventSource(final String channelName) {
        return LOWER_HYPHEN.to(LOWER_CAMEL, channelName) + "EventSource";
    }

    public static String beanNameForMessageLogReceiverEndpoint(final String channelName) {
        return LOWER_HYPHEN.to(LOWER_CAMEL, channelName) + "MessageLogReceiverEndpoint";
    }

    public static String beanNameForMessageQueue(final String channelName) {
        return LOWER_HYPHEN.to(LOWER_CAMEL, channelName) + "MessageQueue";
    }

    public static String beanNameForMessageQueueReceiverEndpoint(final String channelName) {
        return LOWER_HYPHEN.to(LOWER_CAMEL, channelName) + "MessageQueueReceiverEndpoint";
    }

}

