package de.otto.synapse.edison.statusdetail;

import de.otto.edison.status.domain.Status;
import de.otto.edison.status.domain.StatusDetail;
import de.otto.synapse.state.StateRepository;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static de.otto.edison.status.domain.StatusDetail.statusDetail;
import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

public class StateRepositoryStatusDetailIndicatorTest {

    private StateRepositoryStatusDetailIndicator stateRepositoryStatusDetailIndicator;

    @Before
    public void setup() {
        StateRepository mockStateRepository = mock(StateRepository.class);
        stateRepositoryStatusDetailIndicator = new StateRepositoryStatusDetailIndicator(mockStateRepository, "SomeObjects");
        Mockito.when(mockStateRepository.size()).thenReturn(42l);
    }

    @Test
    public void shouldReturnStatusDetailsWhenRunning() {
        // given
        stateRepositoryStatusDetailIndicator.start();

        // when
        List<StatusDetail> statusDetails = stateRepositoryStatusDetailIndicator.statusDetails();

        // then
        assertThat(statusDetails, hasSize(1));
        assertThat(statusDetails.get(0), is(statusDetail("StateRepository 'SomeObjects'", Status.OK, "StateRepository contains 42 elements.")));
    }

    @Test
    public void shouldReturnEmptyListWhenNotRunning() {
        // given
        stateRepositoryStatusDetailIndicator.stop();

        // when
        List<StatusDetail> statusDetails = stateRepositoryStatusDetailIndicator.statusDetails();

        // then
        assertThat(statusDetails, is(emptyList()));
    }

    @Test
    public void shouldBeRunningWhenStarted() {
        // given
        assertFalse(stateRepositoryStatusDetailIndicator.isRunning());

        // when
        stateRepositoryStatusDetailIndicator.start();

        //then
        assertTrue(stateRepositoryStatusDetailIndicator.isRunning());
    }

    @Test
    public void shouldNotBeRunningWhenStopped() {
        // given
        stateRepositoryStatusDetailIndicator.start();

        // when
        stateRepositoryStatusDetailIndicator.stop();

        //then
        assertFalse(stateRepositoryStatusDetailIndicator.isRunning());
    }

    @Test
    public void shouldCallCallbackWhenStopping() {
        // given
        stateRepositoryStatusDetailIndicator.start();
        AtomicBoolean callbackCalled = new AtomicBoolean(false);

        // when
        stateRepositoryStatusDetailIndicator.stop(
                () -> callbackCalled.set(true)
        );

        // then
        assertFalse(stateRepositoryStatusDetailIndicator.isRunning());
        assertTrue(callbackCalled.get());
    }

}