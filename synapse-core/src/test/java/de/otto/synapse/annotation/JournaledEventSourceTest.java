package de.otto.synapse.annotation;

import com.google.common.collect.ImmutableList;
import de.otto.synapse.configuration.InMemoryMessageLogTestConfiguration;
import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.endpoint.MessageInterceptorRegistration;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.journal.Journal;
import de.otto.synapse.journal.JournalingInterceptor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;

import static de.otto.synapse.journal.Journals.multiChannelJournal;
import static de.otto.synapse.journal.Journals.singleChannelJournal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class JournaledEventSourceTest {

    private AnnotationConfigApplicationContext context;

    @Before
    public void init() {
        context = new AnnotationConfigApplicationContext();
        context.register(InMemoryMessageLogTestConfiguration.class);
        TestPropertyValues.of(
                "synapse.receiver.default-headers.enabled=false"
        ).applyTo(context);

    }

    @After
    public void close() {
        if (this.context != null) {
            this.context.close();
        }
    }

    @Test
    public void shouldRegisterJournalingMessageInterceptor() {
        context.register(SingleEventSourceWithSingleJournalConfiguration.class);
        context.refresh();

        final MessageInterceptorRegistry registry = context.getBean(MessageInterceptorRegistry.class);

        final ImmutableList<MessageInterceptorRegistration> registrations = registry.getRegistrations("some-channel", EndpointType.RECEIVER);
        assertThat(registrations, hasSize(1));

        final MessageInterceptorRegistration registration = registrations.get(0);
        assertThat(registration.isEnabledFor("some-channel", EndpointType.RECEIVER), is(true));
        assertThat(registration.getInterceptor(), is(instanceOf(JournalingInterceptor.class)));
        assertThat(((JournalingInterceptor)registration.getInterceptor()).getJournal().getName(), is("someJournal"));
    }

    @Test
    public void shouldRegisterInterceptorsForMultipleJournalsAndSingleChannel() {
        context.register(SingleEventSourceWithMultipleJournalConfiguration.class);
        context.refresh();

        final MessageInterceptorRegistry registry = context.getBean(MessageInterceptorRegistry.class);

        final ImmutableList<MessageInterceptorRegistration> registrations = registry.getRegistrations("some-channel", EndpointType.RECEIVER);
        assertThat(registrations, hasSize(2));

        final MessageInterceptorRegistration first = registrations.get(0);
        assertThat(first.isEnabledFor("some-channel", EndpointType.RECEIVER), is(true));
        assertThat(first.getInterceptor(), is(instanceOf(JournalingInterceptor.class)));
        assertThat(((JournalingInterceptor)first.getInterceptor()).getJournal().getName(), is("someJournal"));

        final MessageInterceptorRegistration second = registrations.get(1);
        assertThat(second.isEnabledFor("some-channel", EndpointType.RECEIVER), is(true));
        assertThat(second.getInterceptor(), is(instanceOf(JournalingInterceptor.class)));
        assertThat(((JournalingInterceptor)second.getInterceptor()).getJournal().getName(), is("otherJournal"));
    }

    @Test
    public void shouldRegisterInterceptorsForMultipleJournalsAndMultipleChannels() {
        context.register(TwoEventSourcesWithThreeJournalsConfiguration.class);
        context.refresh();

        final MessageInterceptorRegistry registry = context.getBean(MessageInterceptorRegistry.class);

        ImmutableList<MessageInterceptorRegistration> registrations = registry.getRegistrations("some-channel", EndpointType.RECEIVER);
        assertThat(registrations, hasSize(2));

        final MessageInterceptorRegistration first = registrations.get(0);
        assertThat(first.isEnabledFor("some-channel", EndpointType.RECEIVER), is(true));
        assertThat(first.getInterceptor(), is(instanceOf(JournalingInterceptor.class)));
        assertThat(((JournalingInterceptor)first.getInterceptor()).getJournal().getName(), is("firstJournal"));

        final MessageInterceptorRegistration second = registrations.get(1);
        assertThat(second.isEnabledFor("some-channel", EndpointType.RECEIVER), is(true));
        assertThat(second.getInterceptor(), is(instanceOf(JournalingInterceptor.class)));
        assertThat(((JournalingInterceptor)second.getInterceptor()).getJournal().getName(), is("secondJournal"));

        registrations = registry.getRegistrations("other-channel", EndpointType.RECEIVER);
        assertThat(registrations, hasSize(2));

        final MessageInterceptorRegistration firstOther = registrations.get(0);
        assertThat(firstOther.isEnabledFor("other-channel", EndpointType.RECEIVER), is(true));
        assertThat(firstOther.getInterceptor(), is(instanceOf(JournalingInterceptor.class)));
        assertThat(((JournalingInterceptor)firstOther.getInterceptor()).getJournal().getName(), is("secondJournal"));

        final MessageInterceptorRegistration secondOther = registrations.get(1);
        assertThat(secondOther.isEnabledFor("other-channel", EndpointType.RECEIVER), is(true));
        assertThat(secondOther.getInterceptor(), is(instanceOf(JournalingInterceptor.class)));
        assertThat(((JournalingInterceptor)secondOther.getInterceptor()).getJournal().getName(), is("thirdJournal"));
    }

    @EnableEventSource(name = "testEventSource", channelName = "some-channel")
    static class SingleEventSourceWithSingleJournalConfiguration {

        @Bean
        public Journal someJournal() {
            return singleChannelJournal("someJournal", "some-channel");
        }
    }

    @EnableEventSource(name = "testEventSource", channelName = "some-channel")
    static class SingleEventSourceWithMultipleJournalConfiguration {

        @Bean
        public Journal someJournal() {
            return singleChannelJournal("someJournal", "some-channel");
        }

        @Bean
        public Journal otherJournal() {
            return singleChannelJournal("otherJournal", "some-channel");
        }
    }

    @EnableEventSource(channelName = "some-channel")
    @EnableEventSource(channelName = "other-channel")
    static class TwoEventSourcesWithThreeJournalsConfiguration {

        @Bean
        public Journal firstJournal() {
            return singleChannelJournal("firstJournal", "some-channel");
        }

        @Bean
        public Journal secondJournal() {
            return multiChannelJournal("secondJournal", "some-channel", "other-channel");
        }

        @Bean
        public Journal thirdJournal() {
            return singleChannelJournal("thirdJournal", "other-channel");
        }

    }

    @EnableEventSource(name = "testEventSource", channelName = "other-channel")
    static class MissingEventSourceWithSingleJournalConfiguration {

        @Bean
        public Journal someJournal() {
            return singleChannelJournal("someJournal", "some-channel");
        }
    }


}
