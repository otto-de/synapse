package de.otto.synapse.edison.state;

import com.damnhandy.uri.template.UriTemplate;
import com.google.common.collect.ImmutableMap;
import de.otto.edison.hal.Link;
import de.otto.edison.hal.Links;
import de.otto.edison.hal.paging.PagingRel;
import de.otto.edison.navigation.NavBar;
import de.otto.synapse.journal.JournalRegistry;
import de.otto.synapse.state.StateRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.View;
import org.springframework.web.servlet.view.RedirectView;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.damnhandy.uri.template.UriTemplate.fromTemplate;
import static com.google.common.collect.Maps.uniqueIndex;
import static de.otto.edison.hal.Links.emptyLinks;
import static de.otto.edison.hal.paging.NumberedPaging.zeroBasedNumberedPaging;
import static de.otto.edison.navigation.NavBarItem.navBarItem;
import static de.otto.synapse.edison.state.PagerModel.UNAVAILABLE;
import static de.otto.synapse.translator.JsonHelper.prettyPrint;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
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
public class StateRepositoryUiController {

    private final JournalRegistry journals;
    private final ImmutableMap<String,StateRepository<?>> stateRepositories;
    private final String managementBasePath;

    public StateRepositoryUiController(final List<StateRepository<?>> stateRepositories,
                                       final JournalRegistry journals,
                                       final NavBar rightNavBar,
                                       final EdisonStateRepositoryUiProperties properties,
                                       final @Value("${edison.application.management.base-path:internal}") String managementBasePath) {
        this.stateRepositories = uniqueIndex(stateRepositories
                .stream()
                .filter(repo -> !properties.getExcluded().contains(repo.getName()))
                .collect(Collectors.toSet()), StateRepository::getName);
        this.journals = journals;
        this.managementBasePath = managementBasePath;
        this.stateRepositories.forEach((repositoryName, _repository) ->
                rightNavBar.register(navBarItem(
                        15,
                        "State Repository: " + repositoryName,
                        format("/%s/staterepositories/%s", managementBasePath, repositoryName))));
    }

    @GetMapping(
            path = "${edison.application.management.base-path:internal}/staterepositories/{repositoryName}",
            produces = "text/html"
    )
    public ModelAndView getStateRepositoryHtml(final @PathVariable String repositoryName,
                                               final @RequestParam(defaultValue = "0") int page,
                                               final @RequestParam(defaultValue = "100") int pageSize,
                                               final UriComponentsBuilder uriComponentsBuilder) {
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
                    ? zeroBasedNumberedPaging(page, pageSize, (int)stateRepository.size()).links(repositoryUri, allOf(PagingRel.class))
                    : emptyLinks());

            final List<ImmutableMap<String, String>> entitiesModel = entityPageIds
                    .stream()
                    .map(entityId -> toEntityModel(entityId, stateRepository.get(entityId)))
                    .collect(toList());
            return new ModelAndView(
                    "staterepository",
                    ImmutableMap.<String,Object>builder()
                            .put("basePath", managementBasePath)
                            .put("singleEntity", false)
                            .put("journaled", journals.hasJournal(repositoryName))
                            .put("repositoryName", repositoryName)
                            .put("entities", entitiesModel)
                            .put("pager", pagerModel)
                            .build()
            );
        } else {
            throw new ResponseStatusException(NOT_FOUND, "No such StateRepository " + repositoryName);
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
        if (stateRepository != null) {

            List<ImmutableMap<String,String>> entities = stateRepository.get(entityId).isPresent()
                    ? singletonList(toEntityModel(entityId, stateRepository.get(entityId).get()))
                    : emptyList();

            return new ModelAndView(
                    "staterepository",
                    ImmutableMap.<String,Object>builder()
                            .put("basePath", managementBasePath)
                            .put("singleEntity", true)
                            .put("journaled", journals.hasJournal(repositoryName))
                            .put("repositoryName", repositoryName)
                            .put("entities", entities)
                            .put("pager", UNAVAILABLE)
                            .build()
            );
        } else {
            throw new ResponseStatusException(NOT_FOUND, "No such StateRepository " + repositoryName);
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

}
