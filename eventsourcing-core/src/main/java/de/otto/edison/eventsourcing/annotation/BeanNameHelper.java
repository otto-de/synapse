package de.otto.edison.eventsourcing.annotation;

import com.google.common.base.CaseFormat;

public class BeanNameHelper {

    public static String beanNameForStream(final String streamName) {
        return CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_CAMEL, streamName) + "EventSource";
    }

}
