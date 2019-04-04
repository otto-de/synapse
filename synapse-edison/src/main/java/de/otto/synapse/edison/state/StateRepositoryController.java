package de.otto.synapse.edison.state;

import com.damnhandy.uri.template.UriTemplate;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import de.otto.edison.hal.HalRepresentation;
import de.otto.edison.hal.Link;
import de.otto.edison.hal.Links;
import de.otto.edison.hal.paging.PagingRel;
import de.otto.edison.navigation.NavBar;
import de.otto.synapse.state.StateRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.View;
import org.springframework.web.servlet.view.RedirectView;
import org.springframework.web.util.UriComponentsBuilder;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.damnhandy.uri.template.UriTemplate.fromTemplate;
import static com.google.common.collect.Maps.uniqueIndex;
import static de.otto.edison.hal.Link.collection;
import static de.otto.edison.hal.Link.item;
import static de.otto.edison.hal.Links.emptyLinks;
import static de.otto.edison.hal.Links.linkingTo;
import static de.otto.edison.hal.paging.NumberedPaging.zeroBasedNumberedPaging;
import static de.otto.edison.navigation.NavBarItem.navBarItem;
import static de.otto.synapse.edison.state.PagerModel.UNAVAILABLE;
import static de.otto.synapse.translator.ObjectMappers.currentObjectMapper;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.EnumSet.allOf;
import static java.util.stream.Collectors.toList;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@Controller
@ConditionalOnBean(
        StateRepository.class
)
@ConditionalOnProperty(
        prefix = "synapse.edison.state.ui",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true)
public class StateRepositoryController {

    private final ImmutableMap<String,StateRepository<?>> stateRepositories;
    private final String managementBasePath;

    public StateRepositoryController(final List<StateRepository<?>> stateRepositories,
                                     final NavBar rightNavBar,
                                     final EdisonStateRepositoryUiProperties properties,
                                     final @Value("${edison.application.management.base-path:internal}") String managementBasePath) {
        this.stateRepositories = uniqueIndex(stateRepositories
                .stream()
                .filter(repo -> !properties.getExcluded().contains(repo.getName()))
                .collect(Collectors.toSet()), StateRepository::getName);
        this.managementBasePath = managementBasePath;
        this.stateRepositories.forEach((repositoryName, _repository) ->
                rightNavBar.register(navBarItem(
                        15,
                        "StateRepository: " + repositoryName,
                        format("/%s/staterepositories/%s", managementBasePath, repositoryName))));
    }

    @GetMapping(
            path = "${edison.application.management.base-path:internal}/staterepositories",
            produces = {"application/hal+json", "application/json"}
    )
    @ResponseBody
    public HalRepresentation getStateRepositories(final HttpServletRequest request) throws JsonProcessingException {
        final String selfHref = request.getRequestURL().toString();
        final List<Link> itemLinks = repositoryLinks(selfHref);
        return new HalRepresentation(
                linkingTo()
                        .self(selfHref)
                        .array(itemLinks)
                        .build()
        );
    }

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
                    .collect(Collectors.toList());

            final Links pagingLinks = pageSize > 0
                    ? zeroBasedNumberedPaging(page, pageSize, allEntityIds.size()).links(repositoryUri, allOf(PagingRel.class))
                    : emptyLinks();

            final List<Link> itemLinks = entityItemLinks(entityUri, entityPageIds);

            return new HalRepresentation(
                    linkingTo()
                            .with(pagingLinks)
                            .single(collection(repositoriesUri.expand()))
                            .array(itemLinks)
                            .build()
            );
        } else {
            throw new HttpClientErrorException(NOT_FOUND, "No such StateRepository " + repositoryName);
        }
    }

    @GetMapping(
            path = "${edison.application.management.base-path:internal}/staterepositories/{repositoryName}",
            produces = "text/html"
    )
    public ModelAndView getStateRepositoryHtml(final @PathVariable String repositoryName,
                                               final @RequestParam(defaultValue = "0") int page,
                                               final @RequestParam(defaultValue = "100") int pageSize,
                                               final UriComponentsBuilder uriComponentsBuilder) throws JsonProcessingException {
        if (stateRepositories.containsKey(repositoryName)) {

            final StateRepository<?> stateRepository = stateRepositories
                    .get(repositoryName);
            final Set<String> allEntityIds = stateRepository
                    .keySet();
            final List<String> entityPageIds = allEntityIds
                    .stream()
                    .skip(page * pageSize)
                    .limit(pageSize)
                    .collect(Collectors.toList());

            final UriComponentsBuilder baseUriBuilder = uriComponentsBuilder
                    .pathSegment(managementBasePath)
                    .path("/staterepositories");

            final UriTemplate repositoryUri = fromTemplate(baseUriBuilder.toUriString() + "/" + repositoryName + "{?page,pageSize}");

            final PagerModel pagerModel = toPagerModel(pageSize > 0
                    ? zeroBasedNumberedPaging(page, pageSize, allEntityIds.size()).links(repositoryUri, allOf(PagingRel.class))
                    : emptyLinks());

            final List<ImmutableMap<String, String>> entitiesModel = entityPageIds
                    .stream()
                    .map(entityId -> toEntityModel(entityId, stateRepository.get(entityId)))
                    .collect(toList());
            return new ModelAndView(
                    "staterepository",
                    ImmutableMap.of(
                            "basePath", managementBasePath,
                            "singleEntity", false,
                            "repositoryName", repositoryName,
                            "entities", entitiesModel,
                            "pager", pagerModel
                    )
            );
        } else {
            throw new HttpClientErrorException(NOT_FOUND, "No such StateRepository " + repositoryName);
        }
    }

    @GetMapping(
            path = "${edison.application.management.base-path:internal}/staterepositories/{repositoryName}/{entityId}",
            produces = {"application/hal+json", "application/json"}
    )
    @ResponseBody
    public String getEntityJson(final @PathVariable String repositoryName,
                                final @PathVariable String entityId) {
        if (stateRepositories.containsKey(repositoryName)) {
            return prettyPrint(stateRepositories.get(repositoryName).get(entityId));
        } else {
            throw new HttpClientErrorException(NOT_FOUND, "No such StateRepository " + repositoryName);
        }
    }

    @GetMapping(
            path = "${edison.application.management.base-path:internal}/staterepositories/{repositoryName}",
            params = "entityId",
            produces = "text/html"
    )
    public View getEntityHtmlRedirect(final @PathVariable String repositoryName,
                                      final @RequestParam String entityId) {
        final String redirectUrl = "/" + managementBasePath + "/staterepositories/" + repositoryName + "/" + entityId;
        return new RedirectView(redirectUrl, true);
    }

    @GetMapping(
            path = "${edison.application.management.base-path:internal}/staterepositories/{repositoryName}/{entityId}",
            produces = "text/html"
    )
    public ModelAndView getEntityHtml(final @PathVariable String repositoryName,
                                      final @PathVariable String entityId) {
        final StateRepository<?> stateRepository = stateRepositories.get(repositoryName);
        if (stateRepository != null && stateRepository.get(entityId).isPresent()) {
            return new ModelAndView(
                    "staterepository",
                    ImmutableMap.of(
                            "basePath", managementBasePath,
                            "singleEntity", true,
                            "repositoryName", repositoryName,
                            "entities", singletonList(
                                    toEntityModel(entityId, stateRepository.get(entityId).get())),
                            "pager", UNAVAILABLE
                    )
            );
        } else {
            throw new HttpClientErrorException(NOT_FOUND, "No such StateRepository " + repositoryName);
        }
    }

    private ImmutableMap<String, String> toEntityModel(final String entityId, final Object entity) {
        return ImmutableMap.of(
                "entityId", entityId,
                "entityJson", prettyPrint(entity));
    }

    private PagerModel toPagerModel(final Links pagingLinks) {
        return new PagerModel(
                pagingLinks.getLinkBy("self").map(Link::getHref).orElse(""),
                pagingLinks.getLinkBy("first").map(Link::getHref).orElse(null),
                pagingLinks.getLinkBy("prev").map(Link::getHref).orElse(null),
                pagingLinks.getLinkBy("next").map(Link::getHref).orElse(null),
                pagingLinks.getLinkBy("last").map(Link::getHref).orElse(null)
        );
    }

    private List<Link> repositoryLinks(final String baseHref) {
        return stateRepositories
                .keySet()
                .stream()
                .sorted()
                .map(repositoryName -> item(repositoryHref(baseHref, repositoryName)))
                .collect(toList());
    }

    private List<Link> entityItemLinks(final UriTemplate uriTemplate,
                                       final List<String> entityIds) {

        return entityIds
                .stream()
                .map(key -> item(uriTemplate.set("entityId", key).expand()))
                .collect(toList());
    }

    private String repositoryHref(final String baseHref,
                                  final String repositoryName) {
        return baseHref.endsWith("/")
                ? baseHref + repositoryName
                : baseHref + "/" + repositoryName;
    }

    private String prettyPrint(final Object entity) {
        if (entity == null) {
            return "null";
        }
        try {
            return currentObjectMapper()
                    .writerWithDefaultPrettyPrinter()
                    .writeValueAsString(entity);
        } catch (final JsonProcessingException e) {
            throw new IllegalStateException("Unable to transform entity to JSON: " + e.getMessage());
        }
    }

}
