package de.otto.synapse.edison.journal;

import com.google.common.collect.ImmutableMap;
import de.otto.edison.hal.HalRepresentation;
import de.otto.edison.hal.Links;
import de.otto.synapse.edison.state.EdisonStateRepositoryUiProperties;
import de.otto.synapse.journal.Journal;
import de.otto.synapse.journal.Journals;
import de.otto.synapse.state.StateRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.collect.Maps.uniqueIndex;
import static de.otto.edison.hal.Link.collection;
import static de.otto.edison.hal.Link.link;
import static de.otto.edison.hal.Links.linkingTo;
import static java.util.Collections.emptyList;
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
public class JournalRestController {

    private final Journals journals;
    private final ImmutableMap<String,StateRepository<?>> stateRepositories;
    private final String managementBasePath;

    public JournalRestController(final List<StateRepository<?>> stateRepositories,
                                 final Journals journals,
                                 final EdisonStateRepositoryUiProperties properties,
                                 final @Value("${edison.application.management.base-path:internal}") String managementBasePath) {
        this.stateRepositories = uniqueIndex(stateRepositories
                .stream()
                .filter(repo -> !properties.getExcluded().contains(repo.getName()))
                .collect(Collectors.toSet()), StateRepository::getName);
        this.journals = journals;
        this.managementBasePath = managementBasePath;
    }

    /**
     * Returns an application/hal+json representation of the event journal of a single event-sourced entity.
     *
     * @param repositoryName the name of the {@link Journal}
     * @param entityId the id of the requested entity
     * @param uriComponentsBuilder builder used to create hrefs
     *
     * @return HalRepresentation the representation of the journal
     */
    @GetMapping(
            path = "${edison.application.management.base-path:internal}/journals/{repositoryName}/{entityId}",
            produces = {"application/hal+json", "application/json"}
    )
    @ResponseBody
    public HalRepresentation getEntityJournalJson(final @PathVariable String repositoryName,
                                                  final @PathVariable String entityId,
                                                  final UriComponentsBuilder uriComponentsBuilder) {
        final String baseUri = uriComponentsBuilder.pathSegment(managementBasePath).toUriString();
        final String selfUri = baseUri + "/journals/" + repositoryName + "/" + entityId;

        if (journals.containsKey(repositoryName)) {
            final List<MessageStoreEntryRepresentation> messages =
                journals.getJournal(repositoryName)
                        .map(journal -> journal.getJournalFor(entityId)
                                .map(MessageStoreEntryRepresentation::new)
                                .collect(toList()))
                        .orElse(emptyList());
            final Links.Builder links = linkingTo()
                    .self(selfUri);
            if (stateRepositories.containsKey(repositoryName)) {
                links
                        .single(
                                link("working-copy", baseUri + "/staterepositories/" + repositoryName + "/" + entityId))
                        .single(
                                collection(baseUri + "/staterepositories/" + repositoryName + "{?page,pageSize}"));
            }
            return new JournalHalRepresentation(
                    links.build(),
                    messages);
        } else {
            throw new ResponseStatusException(NOT_FOUND, "No such Journal " + repositoryName);
        }
    }

}
