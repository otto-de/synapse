package de.otto.synapse.edison.trace;

import de.otto.edison.navigation.NavBar;
import de.otto.synapse.message.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.autoconfigure.ManagementServerProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseBody;

import static de.otto.edison.navigation.NavBarItem.navBarItem;
import static java.util.stream.Collectors.joining;

@Controller
@ConditionalOnBean(name = "traceMessageStore")
public class MessageTraceController {

    private final MessageTraces messageTraces;

    @Autowired
    MessageTraceController(final MessageTraces messageTraces,
                           final NavBar rightNavBar,
                           final ManagementServerProperties managementServerProperties) {
        this.messageTraces = messageTraces;
        rightNavBar.register(
                navBarItem(10, "Message Trace", String.format("%s/message-trace", managementServerProperties.getContextPath()))
        );
    }

    @GetMapping(
            path = "${management.context-path}/messagetrace/receivers/{channelName}",
            produces = "text/html"
    )
    @ResponseBody
    public String getMessageTrace(final @PathVariable String channelName) {
        return messageTraces
                .getReceiverTrace(channelName)
                .stream()
                .map(Message::toString)
                .collect(joining("<li>", "<ol>", "</ol>"));
    }
}
