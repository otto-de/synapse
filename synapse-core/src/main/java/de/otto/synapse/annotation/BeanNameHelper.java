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

    public static String beanNameForMessageQueueReceiverEndpoint(final String channelName) {
        return LOWER_HYPHEN.to(LOWER_CAMEL, channelName) + "MessageQueueReceiverEndpoint";
    }

    public static String beanNameForMessageSenderEndpoint(final String channelName) {
        return LOWER_HYPHEN.to(LOWER_CAMEL, channelName) + "MessageSenderEndpoint";
    }

    public static String beanNameForJournal(final String channelName) {
        return LOWER_HYPHEN.to(LOWER_CAMEL, channelName) + "Journal";
    }

    public static String beanNameForIteratorAt(final String iteratorAt) {
        return LOWER_HYPHEN.to(LOWER_CAMEL, iteratorAt) + "IteratorAt";
    }

}

