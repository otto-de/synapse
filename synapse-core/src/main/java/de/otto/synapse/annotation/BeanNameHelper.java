package de.otto.synapse.annotation;

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_HYPHEN;

public class BeanNameHelper {

    public static String beanNameForStream(final String channelName) {
        return LOWER_HYPHEN.to(LOWER_CAMEL, channelName) + "EventSource";
    }

}
