package de.otto.synapse.annotation;

import java.lang.annotation.Annotation;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_HYPHEN;

public class BeanNameHelper {

    public static String beanNameFor(final Class<? extends Annotation> annotationType,
                                     final String channelName) {
        switch (annotationType.getSimpleName()) {
            case "EnableMessageLogReceiverEndpoint":
            case "EnableMessageLogReceiverEndpoints":
                return beanNameForMessageLogReceiverEndpoint(channelName);
            case "EnableEventSource":
            case "EnableEventSources":
                return beanNameForEventSource(channelName);
            case "EnableMessageQueueReceiverEndpoint":
            case "EnableMessageQueueReceiverEndpoints":
                return beanNameForMessageQueueReceiverEndpoint(channelName);
            case "EnableMessageSenderEndpoint":
            case "EnableMessageSenderEndpoints":
                return beanNameForMessageSenderEndpoint(channelName);
            default:
                throw new IllegalArgumentException("Unable to resolve bean name for " + annotationType.getName());
        }
    }

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

}

