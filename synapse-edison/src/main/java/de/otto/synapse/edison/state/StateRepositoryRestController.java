package de.otto.synapse.edison.state;

import com.damnhandy.uri.template.UriTemplate;
import com.google.common.collect.ImmutableMap;
import de.otto.edison.hal.HalRepresentation;
import de.otto.edison.hal.Link;
import de.otto.edison.hal.Links;
import de.otto.edison.hal.paging.PagingRel;
import de.otto.synapse.journal.Journal;
import de.otto.synapse.journal.JournalRegistry;
import de.otto.synapse.state.StateRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.util.UriComponentsBuilder;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.damnhandy.uri.template.UriTemplate.fromTemplate;
import static com.google.common.collect.Maps.uniqueIndex;
import static de.otto.edison.hal.Link.*;
import static de.otto.edison.hal.Links.emptyLinks;
import static de.otto.edison.hal.Links.linkingTo;
import static de.otto.edison.hal.paging.NumberedPaging.zeroBasedNumberedPaging;
import static java.util.EnumSet.allOf;
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
public class StateRepositoryRestController {

    private final JournalRegistry journals;
    private final ImmutableMap<String,StateRepository<?>> stateRepositories;
    private final String managementBasePath;

    public StateRepositoryRestController(final List<StateRepository<?>> stateRepositories,
                                         final JournalRegistry journals,
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
     * Returns an application/hal+json representation of the container-resource containing all staterepositories that
     * are not {@link EdisonStateRepositoryUiProperties#getExcluded() excluded} from the state-repository UI.
     *
     * @param request current HttpServletRequest
     * @return HalRepresentation of the collection of state repositories
     */
    @GetMapping(
            path = "${edison.application.management.base-path:internal}/staterepositories",
            produces = {"application/hal+json", "application/json"}
    )
    @ResponseBody
    public HalRepresentation getStateRepositories(final HttpServletRequest request) {
        final String selfHref = request.getRequestURL().toString();
        final List<Link> itemLinks = itemLinks(selfHref);
        return new HalRepresentation(
                linkingTo()
                        .self(selfHref)
                        .array(itemLinks)
                        .build()
        );
    }

    /**
     * Returns an application/hal+json representation of a {@link StateRepository}, containing a pageable collection
     * resource with links to the event-sourced entities stored in the repository.
     *
     * @param repositoryName the name of the StateRepository
     * @param page the zero-based page number
     * @param pageSize the number of entities to return
     * @param uriComponentsBuilder builder used to create hrefs
     *
     * @return HalRepresentation of the paged collection resource
     */
    @GetMapping(
            path = "${edison.application.management.base-path:internal}/staterepositories/{repositoryName}",
            produces = {"application/hal+json", "application/json"}
    )
    @ResponseBody
    public HalRepresentation getStateRepository(final @PathVariable String repositoryName,
                                                final @RequestParam(defaultValue = "0") int page,
                                                final @RequestParam(defaultValue = "100") int pageSize,
                                                final UriComponentsBuilder uriComponentsBuilder) {
        if (stateRepositories.containsKey(repositoryName)) {

            final UriComponentsBuilder baseUriBuilder = uriComponentsBuilder
                    .pathSegment(managementBasePath)
                    .path("/staterepositories");
            final UriTemplate repositoriesUri = fromTemplate(baseUriBuilder.toUriString());
            final UriTemplate repositoryUri = fromTemplate(baseUriBuilder.toUriString() + "/" + repositoryName + "{?page,pageSize}");
            final UriTemplate entityUri = fromTemplate(baseUriBuilder.toUriString() + "/" + repositoryName + "/{entityId}");

            final Set<String> allEntityIds = stateRepositories
                    .get(repositoryName)
                    .keySet();
            final List<String> entityPageIds = allEntityIds
                    .stream()
                    .skip(page * pageSize)
                    .limit(pageSize)
                    .collect(toList());

            final Links pagingLinks = pageSize > 0
                    ? zeroBasedNumberedPaging(page, pageSize, allEntityIds.size()).links(repositoryUri, allOf(PagingRel.class))
                    : emptyLinks();

            final List<Link> itemLinks = entityItemLinks(entityUri, entityPageIds);

            return new HalRepresentation(
                    linkingTo()
                            .with(pagingLinks)
                            .single(collection(repositoriesUri.expand()))
                            .array(itemLinks).build()
            );
        } else {
            throw new ResponseStatusException(NOT_FOUND, "No such StateRepository " + repositoryName);
        }
    }

    /**
     * Returns an application/hal+json representation of a single event-sourced entity stored in the
     * {@link StateRepository} with the specified {@code repositoryName}.
     *
     * <p>If the state repository has a {@link Journal}, the returned
     * entity representation will contain a link to the messages that where leading to the current state of the
     * entity.</p>
     *
     * @param repositoryName the name of the {@code StateRepository}
     * @param entityId the id of the requested entity
     * @param uriComponentsBuilder builder used to create hrefs
     *
     * @return HalRepresentation the representation of the entity
     */
    @GetMapping(
            path = "${edison.application.management.base-path:internal}/staterepositories/{repositoryName}/{entityId}",
            produces = {"application/hal+json", "application/json"}
    )
    @ResponseBody
    public HalRepresentation getEntityJson(final @PathVariable String repositoryName,
                                           final @PathVariable String entityId,
                                           final UriComponentsBuilder uriComponentsBuilder) {
        final String baseUri = uriComponentsBuilder.pathSegment(managementBasePath).toUriString();

        if (stateRepositories.containsKey(repositoryName)) {
            final StateRepository<?> stateRepository = stateRepositories.get(repositoryName);
            final Links.Builder links = linkingTo()
                    .self(
                            baseUri + "/" + repositoryName + "/staterepositories/" + entityId)

                    .single(
                            collection(baseUri + "/staterepositories/" + repositoryName + "{?page,pageSize}"));
            if (journals.hasJournal(repositoryName)) {
                links.single(
                        link("working-copy-of", baseUri + "/journals/" + repositoryName + "/" + entityId));
            }
            return new EntityHalRepresentation(
                    links.build(),
                    stateRepository.get(entityId));
        } else {
            throw new ResponseStatusException(NOT_FOUND, "No such StateRepository " + repositoryName);
        }
    }

    private List<Link> entityItemLinks(final UriTemplate uriTemplate,
                                       final List<String> entityIds) {

        return entityIds
                .stream()
                .map(key -> item(uriTemplate.set("entityId", key).expand()))
                .collect(toList());
    }

    private List<Link> itemLinks(final String baseHref) {
        return this.stateRepositories
                .keySet()
                .stream()
                .sorted()
                .map(itemId -> item(itemHref(baseHref, itemId)))
                .collect(toList());
    }

    private String itemHref(final String baseHref,
                            final String itemId) {
        return baseHref.endsWith("/")
                ? baseHref + itemId
                : baseHref + "/" + itemId;
    }

}
