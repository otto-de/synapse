package de.otto.synapse.edison.state;

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
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;

public class StatusIndicatingStateRepositoryTest {
    private StatusIndicatingStateRepository statusIndicatingStateRepository;

    @Before
    public void setup() {
        StateRepository mockStateRepository = mock(StateRepository.class);
        statusIndicatingStateRepository = new StatusIndicatingStateRepository(mockStateRepository,"SomeObjects");
        Mockito.when(mockStateRepository.size()).thenReturn(42l);
    }

    @Test
    public void shouldReturnStatusDetailsWhenRunning() {
        // given
        statusIndicatingStateRepository.start();

        // when
        List<StatusDetail> statusDetails = statusIndicatingStateRepository.statusDetails();

        // then
        assertThat(statusDetails, hasSize(1));
        assertThat(statusDetails.get(0), is(statusDetail("StateRepository 'SomeObjects'", Status.OK, "StateRepository contains 42 elements.")));
    }

    @Test
    public void shouldReturnEmptyListWhenNotRunning() {
        // given
        statusIndicatingStateRepository.stop();

        // when
        List<StatusDetail> statusDetails = statusIndicatingStateRepository.statusDetails();

        // then
        assertThat(statusDetails, is(emptyList()));
    }

    @Test
    public void shouldBeRunningWhenStarted() {
        // given
        assertFalse(statusIndicatingStateRepository.isRunning());

        // when
        statusIndicatingStateRepository.start();

        //then
        assertTrue(statusIndicatingStateRepository.isRunning());
    }

    @Test
    public void shouldNotBeRunningWhenStopped() {
        // given
        statusIndicatingStateRepository.start();

        // when
        statusIndicatingStateRepository.stop();

        //then
        assertFalse(statusIndicatingStateRepository.isRunning());
    }

    @Test
    public void shouldCallCallbackWhenStopping() {
        // given
        statusIndicatingStateRepository.start();
        AtomicBoolean callbackCalled = new AtomicBoolean(false);

        // when
        statusIndicatingStateRepository.stop(
                () -> callbackCalled.set(true)
        );

        // then
        assertFalse(statusIndicatingStateRepository.isRunning());
        assertTrue(callbackCalled.get());
    }
}