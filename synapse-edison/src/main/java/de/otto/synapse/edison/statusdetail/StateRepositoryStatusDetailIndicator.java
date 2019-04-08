package de.otto.synapse.edison.statusdetail;

import de.otto.edison.status.domain.Status;
import de.otto.edison.status.domain.StatusDetail;
import de.otto.edison.status.indicator.StatusDetailIndicator;
import de.otto.synapse.state.StateRepository;
import org.springframework.context.SmartLifecycle;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static de.otto.edison.status.domain.StatusDetail.statusDetail;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * This status detail indicator indicates the number of entries in the given state repository.
 */
public class StateRepositoryStatusDetailIndicator implements StatusDetailIndicator, SmartLifecycle {

    private final StateRepository<?> stateRepository;
    private final String repositoryName;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public StateRepositoryStatusDetailIndicator(final StateRepository<?> stateRepository, String repositoryName) {
        this.stateRepository = stateRepository;
        this.repositoryName = repositoryName;
    }

    @Override
    public List<StatusDetail> statusDetails() {
        if (isRunning()) {
            return singletonList(statusDetail("StateRepository '" + repositoryName + "'", Status.OK, String.format("StateRepository contains %s elements.", stateRepository.size())));
        } else {
            return emptyList();
        }
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void start() {
        running.set(true);
    }

    @Override
    public void stop() {
        running.set(false);
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return 0;
    }
}
