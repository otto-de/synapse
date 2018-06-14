package de.otto.synapse.edison.history;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@ConditionalOnProperty(
        prefix = "synapse.edison.history",
        name = "enabled",
        havingValue = "true")
@Configuration
public class HistoryConfiguration {

    @Bean
    public HistoryController historyController() {
        return new HistoryController(historyService());
    }
    
    @Bean
    public HistoryService historyService() {
        return new HistoryService();
    }

}
