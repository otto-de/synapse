package de.otto.synapse.edison.trace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import de.otto.edison.navigation.NavBar;
import de.otto.synapse.channel.ShardPosition;
import de.otto.synapse.message.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.ManagementServerProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.servlet.ModelAndView;

import java.util.LinkedHashMap;

import static de.otto.edison.navigation.NavBarItem.navBarItem;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

@Controller
@ConditionalOnBean(name = "traceMessageStore")
public class MessageTraceController {

    private final MessageTraces messageTraces;
    private final ObjectMapper objectMapper;

    @Autowired
    MessageTraceController(final MessageTraces messageTraces,
                           final NavBar rightNavBar,
                           final ManagementServerProperties managementServerProperties,
                           final ObjectMapper objectMapper) {
        this.messageTraces = messageTraces;
        this.objectMapper = objectMapper;
        messageTraces.getSenderChannels().forEach(channelName -> {
            rightNavBar.register(
                    navBarItem(10, "Sender: " + channelName, format("%s/messagetrace/sender/%s", managementServerProperties.getContextPath(), channelName))
            );
        });
        messageTraces.getReceiverChannels().forEach(channelName -> {
            rightNavBar.register(
                    navBarItem(20, "Receiver: " + channelName, format("%s/messagetrace/receiver/%s", managementServerProperties.getContextPath(), channelName))
            );
        });
    }

    @GetMapping(
            path = "${management.context-path}/messagetrace/{endpointType}/{channelName}",
            produces = "text/html"
    )
    public ModelAndView getMessageTrace(final @PathVariable String endpointType,
                                        final @PathVariable String channelName) {
        return new ModelAndView(
                "messagetrace",
                ImmutableMap.of(
                        "title", endpointType.equals("receiver") ? "Receiver: " : "Sender: " + channelName,
                        "channelName", channelName,
                        "messages",
                        messageTraces
                                .getReceiverTrace(channelName)
                                .stream()
                                .map(message -> ImmutableMap.of(
                                        "key", message.getKey(),
                                        "header", prettyPrint(message.getHeader()),
                                        "payload", prettyPrint(message.getPayload())
                                ))
                                .collect(toList())
                )
        );
    }

    private String prettyPrint(final String json) {
        try {
            Object jsonObject = objectMapper.readValue(json, Object.class);
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonObject);
        } catch (final Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private String prettyPrint(final Header header) {
        try {

            final LinkedHashMap<Object, Object> map = Maps.newLinkedHashMap();
            map.put("shardPosition", header.getShardPosition().map(ShardPosition::toString).orElse(""));
            map.put("arrivalTimestamp", header.getArrivalTimestamp().toString());
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(map);
        } catch (final Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
