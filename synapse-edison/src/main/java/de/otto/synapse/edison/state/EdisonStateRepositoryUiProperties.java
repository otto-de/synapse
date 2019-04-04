package de.otto.synapse.edison.state;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "synapse.edison.state.ui")
public class EdisonStateRepositoryUiProperties {
    /**
     * Enables the registration of Edison UIs for StateRepositories.
     */
    private boolean enabled = true;
    /**
     * The list of StateRepository names that should be excluded from the UI.
     */
    private List<String> excluded = new ArrayList<>();

    EdisonStateRepositoryUiProperties() {
        excluded.add("Compaction");
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public List<String> getExcluded() {
        return excluded;
    }

    public void setExcluded(List<String> excluded) {
        this.excluded = excluded;
    }
}
