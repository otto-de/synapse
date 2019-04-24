package de.otto.synapse.edison.trace;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.message.Header;
import de.otto.synapse.message.Key;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.servlet.ModelAndView;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.LinkedHashMap;

import static de.otto.synapse.translator.ObjectMappers.currentObjectMapper;
import static java.util.stream.Collectors.toList;

@Controller
@ConditionalOnProperty(
        prefix = "synapse.edison.trace",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true)
public class MessageTraceController {

    private final MessageTrace messageTrace;

    @Autowired
    MessageTraceController(final MessageTrace messageTrace) {
        this.messageTrace = messageTrace;
    }

    @GetMapping(
            path = "${edison.application.management.base-path:internal}/messagetrace/{endpointType}/{channelName}",
            produces = "text/html"
    )
    public ModelAndView getMessageTrace(final @PathVariable String endpointType,
                                        final @PathVariable String channelName) {
        return new ModelAndView(
                "single-channel-trace",
                ImmutableMap.of(
                        "endpointType", endpointType.equals("receiver")
                                ? "Receiver"
                                : "Sender",
                        "channelName", channelName,
                        "capacity", messageTrace.getCapacity(),
                        "messages",
                        messageTrace
                                .stream(channelName, EndpointType.valueOf(endpointType.toUpperCase()))
                                .map(traceEntry -> new HashMap() {{
                                    put("sequenceNumber", traceEntry.getSequenceNumber());
                                    put("channelName", channelName);
                                    put("key", traceEntry.getMessage().getKey().toString());
                                    put("header", traceEntry.getMessage().getHeader());
                                    put("payload", traceEntry.getMessage().getPayload());
                                }})
                                .collect(toList())
                )
        );
    }

    @GetMapping(
            path = "${edison.application.management.base-path:internal}/messagetrace",
            produces = "text/html"
    )
    public ModelAndView getMessageTrace() {
        return new ModelAndView(
                "multi-channel-trace",
                ImmutableMap.of(
                        "title",
                        "Message Trace",
                        "messages",
                        messageTrace
                                .stream()
                                .map(traceEntry -> ImmutableMap.builder()
                                        .put("sequenceNumber", traceEntry.getSequenceNumber())
                                        .put("ts", LocalDateTime.ofInstant(traceEntry.getTimestamp(), ZoneId.systemDefault()).toString())
                                        .put("channelName", traceEntry.getChannelName())
                                        .put("endpointType", traceEntry.getEndpointType().name())
                                        .put("key", prettyPrint(traceEntry.getMessage().getKey()))
                                        .put("header", prettyPrint(traceEntry.getMessage().getHeader()))
                                        .put("payload", prettyPrint(traceEntry.getMessage().getPayload()))
                                        .build()
                                )
                                .collect(toList())
                )
        );
    }

    private String prettyPrint(final String json) {
        if (json == null) {
            return "null";
        }
        try {
            Object jsonObject = currentObjectMapper().readValue(json, Object.class);
            if (jsonObject == null) {
                return "null";
            } else {
                return currentObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(jsonObject);
            }
        } catch (final Exception e) {
            return json;
        }
    }

    private String prettyPrint(final Key key) {
        try {
            if (key.compactionKey().equals(key.partitionKey())) {
                return key.partitionKey();
            } else {
                final LinkedHashMap<Object, Object> map = Maps.newLinkedHashMap();
                map.put("partitionKey", key.partitionKey());
                map.put("compactionKey", key.compactionKey());
                return currentObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(map);
            }
        } catch (final Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private String prettyPrint(final Header header) {
        try {

            final LinkedHashMap<Object, Object> map = Maps.newLinkedHashMap();
            map.put("shardPosition", header.getShardPosition().map(ShardPosition::toString).orElse(""));
            map.put("attributes", header.getAll());
            return currentObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(map);
        } catch (final Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
