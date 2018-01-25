package de.otto.edison.eventsourcing;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.Map;

public interface EventSender {

    void sendEvent(String key, Object payload) throws JsonProcessingException;

    void sendEvents(Map<String, Object> events) throws JsonProcessingException;

}
