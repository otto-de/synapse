package de.otto.synapse.edison.state;

import de.otto.edison.status.domain.Status;
import de.otto.edison.status.domain.StatusDetail;
import de.otto.edison.status.indicator.StatusDetailIndicator;
import de.otto.synapse.state.DelegatingStateRepository;
import de.otto.synapse.state.StateRepository;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class StatusIndicatingStateRepository<T> extends DelegatingStateRepository<T> implements StatusDetailIndicator {

    private final String repositoryName;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public StatusIndicatingStateRepository(final StateRepository<T> stateRepository,
                                           final String repositoryName) {
        super(stateRepository);
        this.repositoryName = repositoryName;
    }

    @Override
    public List<StatusDetail> statusDetails() {
        if (running.get()) {
            return singletonList(StatusDetail.statusDetail("StateRepository '" + repositoryName + "'", Status.OK, String.format("StateRepository contains %s elements.", size())));
        } else {
            return emptyList();
        }
    }

    @Override
    public void close() throws Exception {
        running.set(false);
        super.close();
    }

}
