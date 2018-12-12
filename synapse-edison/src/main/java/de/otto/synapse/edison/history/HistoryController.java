package de.otto.synapse.edison.history;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.util.Map;

import static de.otto.synapse.translator.ObjectMappers.currentObjectMapper;
import static java.util.Collections.singletonMap;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.http.ResponseEntity.ok;

@ConditionalOnProperty(
        prefix = "synapse.edison.history",
        name = "enabled",
        havingValue = "true")
@Controller
public class HistoryController {

    private final HistoryService historyService;

    public HistoryController(final HistoryService historyService) {
        this.historyService = historyService;
    }

    @GetMapping(
            value = "${management.context-path}/history/{type}/{entityId}",
            produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, ?>> getHistoryAsJson (final @PathVariable String type,
                                                            final @PathVariable String entityId) {
        return ok(singletonMap("history", currentObjectMapper().convertValue(historyService.getHistory(type, entityId), Map.class)));
    }
}
