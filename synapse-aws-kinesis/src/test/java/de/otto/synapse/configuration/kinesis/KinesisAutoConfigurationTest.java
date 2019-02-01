package de.otto.synapse.configuration.kinesis;

import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import software.amazon.awssdk.core.retry.RetryPolicy;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class KinesisAutoConfigurationTest {

    @Test
    public void shouldRegisterRetryPolicyWithMaxRetries() {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.register(KinesisAutoConfiguration.class);
        context.refresh();

        assertThat(context.containsBean("kinesisRetryPolicy"), is(true));
        RetryPolicy retryPolicy = context.getBean("kinesisRetryPolicy", RetryPolicy.class);
        assertThat(retryPolicy.numRetries(), is(Integer.MAX_VALUE));
    }

}
