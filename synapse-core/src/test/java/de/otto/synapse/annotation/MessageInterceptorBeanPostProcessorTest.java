package de.otto.synapse.annotation;

import de.otto.synapse.configuration.SynapseAutoConfiguration;
import de.otto.synapse.endpoint.EndpointType;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.message.Message;
import de.otto.synapse.message.TextMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.springframework.core.Ordered.LOWEST_PRECEDENCE;

public class MessageInterceptorBeanPostProcessorTest {

    private AnnotationConfigApplicationContext context;

    @Before
    public void init() {
        context = new AnnotationConfigApplicationContext();
        TestPropertyValues.of(
                "synapse.sender.default-headers.enabled=false",
                "synapse.receiver.default-headers.enabled=false")
        .applyTo(context);
    }

    @After
    public void close() {
        if (this.context != null) {
            this.context.close();
        }
    }

    @Test
    public void shouldHaveLowestPrecedence() {
        context.register(MatchAllMessageInterceptorWithVoidResponse.class);
        context.register(SynapseAutoConfiguration.class);
        context.refresh();

        final MessageInterceptorBeanPostProcessor postProcessor = context.getBean(MessageInterceptorBeanPostProcessor.class);

        assertThat(postProcessor.getOrder(), is(LOWEST_PRECEDENCE));
    }

    @Test
    public void shouldRegisterOrderedMessageInterceptors() {
        context.register(OrderedMessageInterceptorWithVoidResponse.class);
        context.register(MatchAllMessageInterceptorWithVoidResponse.class);
        context.register(SynapseAutoConfiguration.class);
        context.refresh();

        final MessageInterceptorRegistry registry = context.getBean(MessageInterceptorRegistry.class);
        assertThat(registry.getRegistrations("foo", EndpointType.SENDER), hasSize(2));
        assertThat(registry.getRegistrations("foo", EndpointType.SENDER).stream().map(Ordered::getOrder).collect(toList()), contains(LOWEST_PRECEDENCE, 42));
        assertThat(registry.getRegistrations("bar", EndpointType.RECEIVER), hasSize(2));
        assertThat(registry.getRegistrations("bar", EndpointType.RECEIVER).stream().map(Ordered::getOrder).collect(toList()), contains(LOWEST_PRECEDENCE, 42));
    }

    @Test
    public void shouldRegisterMatchAllMessageInterceptorWithVoidResponse() {
        context.register(MatchAllMessageInterceptorWithVoidResponse.class);
        context.register(SynapseAutoConfiguration.class);
        context.refresh();

        final MessageInterceptorRegistry registry = context.getBean(MessageInterceptorRegistry.class);
        assertThat(registry.getRegistrations("foo", EndpointType.SENDER), hasSize(1));
        assertThat(registry.getRegistrations("bar", EndpointType.RECEIVER), hasSize(1));
    }

    @Test
    public void shouldRegisterMatchAllMessageInterceptorWithResponse() {
        context.register(MatchAllMessageInterceptorWithMessageResponse.class);
        context.register(SynapseAutoConfiguration.class);
        context.refresh();

        final MessageInterceptorRegistry registry = context.getBean(MessageInterceptorRegistry.class);
        assertThat(registry.getRegistrations("foo", EndpointType.SENDER), hasSize(1));
        assertThat(registry.getRegistrations("bar", EndpointType.RECEIVER), hasSize(1));
    }

    @Test
    public void shouldRegisterTextMessageInterceptorWithTextMessageResponse() {
        context.register(TextMessageInterceptorWithTextMessageResponse.class);
        context.register(SynapseAutoConfiguration.class);
        context.refresh();

        final MessageInterceptorRegistry registry = context.getBean(MessageInterceptorRegistry.class);
        assertThat(registry.getRegistrations("foo", EndpointType.SENDER), hasSize(1));
        assertThat(registry.getRegistrations("bar", EndpointType.RECEIVER), hasSize(1));
    }

    @Test
    public void shouldRegisterMatchKeyAndChannelMessageInterceptor() {
        context.register(MatchKeyAndChannelMessageInterceptor.class);
        context.register(SynapseAutoConfiguration.class);
        context.refresh();

        final MessageInterceptorRegistry registry = context.getBean(MessageInterceptorRegistry.class);
        assertThat(registry.getRegistrations("foo", EndpointType.SENDER), hasSize(1));
        assertThat(registry.getRegistrations("bar", EndpointType.SENDER), hasSize(0));
        assertThat(registry.getRegistrations("foo", EndpointType.RECEIVER), hasSize(0));
    }



    @Test(expected = BeanCreationException.class)
    public void shouldFailToRegisterMatchAllMessageInterceptorWithIllegalSignature1() {
        context.register(MatchAllMessageInterceptorWithIllegalSignature1.class);
        context.register(SynapseAutoConfiguration.class);
        context.refresh();
    }

    @Test(expected = BeanCreationException.class)
    public void shouldFailToRegisterMatchAllMessageInterceptorWithIllegalSignature2() {
        context.register(MatchAllMessageInterceptorWithIllegalSignature2.class);
        context.register(SynapseAutoConfiguration.class);
        context.refresh();
    }

    @Test(expected = BeanCreationException.class)
    public void shouldFailToRegisterMatchAllMessageInterceptorWithIllegalSignature3() {
        context.register(MatchAllMessageInterceptorWithIllegalSignature3.class);
        context.register(SynapseAutoConfiguration.class);
        context.refresh();
    }

    @Test(expected = BeanCreationException.class)
    public void shouldFailToRegisterMatchAllMessageInterceptorWithIllegalSignature4() {
        context.register(MatchAllMessageInterceptorWithIllegalSignature4.class);
        context.register(SynapseAutoConfiguration.class);
        context.refresh();
    }

    @Test(expected = BeanCreationException.class)
    public void shouldFailToRegisterMatchAllMessageInterceptorWithIllegalSignature5() {
        context.register(MatchAllMessageInterceptorWithIllegalSignature5.class);
        context.register(SynapseAutoConfiguration.class);
        context.refresh();
    }


    static class OrderedMessageInterceptorWithVoidResponse {
        @MessageInterceptor
        @Order(42)
        public void test(final Message<String> message) {
        }
    }

    static class MatchAllMessageInterceptorWithVoidResponse {
        @MessageInterceptor
        public void test(final Message<String> message) {
        }
    }

    static class TextMessageInterceptorWithTextMessageResponse {
        @MessageInterceptor
        public TextMessage test(final TextMessage message) {
            return null;
        }
    }

    static class MatchAllMessageInterceptorWithMessageResponse {
        @MessageInterceptor
        public Message<String> test(final Message<String> message) {
            return null;
        }
    }

    static class MatchKeyAndChannelMessageInterceptor {
        @MessageInterceptor(channelNamePattern = "foo", endpointType = EndpointType.SENDER)
        public void test(final Message<String> message) {
        }
    }

    static class MatchAllMessageInterceptorWithIllegalSignature1 {
        @MessageInterceptor
        public Message<Integer> test() {
            return null;
        }
    }

    static class MatchAllMessageInterceptorWithIllegalSignature2 {
        @MessageInterceptor
        public Message<Integer> test(Message wrongType) {
            return null;
        }
    }

    static class MatchAllMessageInterceptorWithIllegalSignature3 {
        @MessageInterceptor
        public Message<Integer> test(String wrongType) {
            return null;
        }
    }

    static class MatchAllMessageInterceptorWithIllegalSignature4 {
        @MessageInterceptor
        public Message<Integer> test(Message<Integer> wrongType) {
            return null;
        }
    }

    static class MatchAllMessageInterceptorWithIllegalSignature5 {
        @MessageInterceptor
        public Message<Integer> test(Message<String> message, String wrongParameter) {
            return null;
        }
    }


}
