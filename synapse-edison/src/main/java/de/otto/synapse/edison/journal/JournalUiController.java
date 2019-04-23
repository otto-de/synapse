package de.otto.synapse.edison.journal;

import com.google.common.collect.ImmutableMap;
import de.otto.synapse.journal.Journal;
import de.otto.synapse.journal.JournalRegistry;
import de.otto.synapse.messagestore.MessageStoreEntry;
import de.otto.synapse.state.StateRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.ModelAndView;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@Controller
@ConditionalOnBean(
        StateRepository.class)
@ConditionalOnProperty(
        prefix = "synapse.edison.state.ui",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true)
public class JournalUiController {

    private final JournalRegistry journals;

    public JournalUiController(final JournalRegistry journals) {
        this.journals = journals;
    }

    /**
     * Returns a HTML UI for the event journal of a single event-sourced entity.
     *
     * @param repositoryName the name of the {@link Journal}
     * @param entityId the id of the requested entity
     *
     * @return HalRepresentation the representation of the journal
     */
    @GetMapping(
            path = "${edison.application.management.base-path:internal}/journals/{repositoryName}/{entityId}",
            produces = {"text/html"}
    )
    public ModelAndView getEntityJournalHtml(final @PathVariable String repositoryName,
                                             final @PathVariable String entityId) {
        if (journals.hasJournal(repositoryName)) {
            final AtomicLong nextSequenceNumber = new AtomicLong(0L);
            final Stream<MessageStoreEntry> entries = journals.getJournal(repositoryName).get().getJournalFor(entityId);
            return new ModelAndView(
                    "journal",
                    ImmutableMap.of(
                            "title", "Journal: " + repositoryName + "/" + entityId,
                            "messages",
                            entries
                                    .map(message -> ImmutableMap.of(
                                            "sequenceNumber", nextSequenceNumber.getAndIncrement(),
                                            "channelName", message.getChannelName(),
                                            "key", message.getTextMessage().getKey().toString(),
                                            "header", message.getTextMessage().getHeader(),
                                            "payload", message.getTextMessage().getPayload()
                                    ))
                                    .collect(toList())
                    )
            );
        } else {
            throw new ResponseStatusException(NOT_FOUND, "No such Journal " + repositoryName);
        }

    }

}
