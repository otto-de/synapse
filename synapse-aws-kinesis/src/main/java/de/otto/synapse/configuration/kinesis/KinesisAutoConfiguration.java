package de.otto.synapse.configuration.kinesis;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import de.otto.synapse.configuration.SynapseAutoConfiguration;
import de.otto.synapse.configuration.aws.AwsProperties;
import de.otto.synapse.configuration.aws.SynapseAwsAuthConfiguration;
import de.otto.synapse.endpoint.MessageInterceptorRegistry;
import de.otto.synapse.endpoint.receiver.MessageLogReceiverEndpointFactory;
import de.otto.synapse.endpoint.receiver.kinesis.KinesisMessageLogReceiverEndpointFactory;
import de.otto.synapse.endpoint.sender.MessageSenderEndpointFactory;
import de.otto.synapse.endpoint.sender.kinesis.KinesisMessageSenderEndpointFactory;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetrySetting;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.RetryPolicyContext;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.core.retry.conditions.OrRetryCondition;
import software.amazon.awssdk.core.retry.conditions.RetryCondition;
import software.amazon.awssdk.core.retry.conditions.RetryOnExceptionsCondition;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;

import java.time.Duration;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.slf4j.LoggerFactory.getLogger;
import static software.amazon.awssdk.core.interceptor.SdkExecutionAttribute.OPERATION_NAME;
import static software.amazon.awssdk.core.interceptor.SdkExecutionAttribute.SERVICE_NAME;

@Configuration
@Import({SynapseAwsAuthConfiguration.class, SynapseAutoConfiguration.class})
@EnableConfigurationProperties(AwsProperties.class)
public class KinesisAutoConfiguration {

    private static final Logger LOG = getLogger(KinesisAutoConfiguration.class);

    private final AwsProperties awsProperties;

    @Autowired
    public KinesisAutoConfiguration(final AwsProperties awsProperties) {
        this.awsProperties = awsProperties;
    }

    @Bean
    @ConditionalOnMissingBean(name = "kinesisRetryPolicy", value = RetryPolicy.class)
    public RetryPolicy kinesisRetryPolicy() {
        return RetryPolicy.defaultRetryPolicy().toBuilder()
                .retryCondition(new DefaultLoggingRetryCondition(5, 10))
                .numRetries(Integer.MAX_VALUE)
                .backoffStrategy(FullJitterBackoffStrategy.builder()
                        .baseDelay(Duration.ofSeconds(1))
                        .maxBackoffTime(SdkDefaultRetrySetting.MAX_BACKOFF)
                        .build())
                .build();
    }

    @Bean
    @ConditionalOnMissingBean(KinesisAsyncClient.class)
    public KinesisAsyncClient kinesisAsyncClient(final AwsCredentialsProvider credentialsProvider,
                                                 final RetryPolicy kinesisRetryPolicy) {
        return KinesisAsyncClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(awsProperties.getRegion()))
                .overrideConfiguration(ClientOverrideConfiguration.builder().retryPolicy(kinesisRetryPolicy).build())
                .build();
    }

    @Bean
    @ConditionalOnMissingBean(name = "kinesisMessageLogSenderEndpointFactory")
    public MessageSenderEndpointFactory kinesisMessageLogSenderEndpointFactory(final MessageInterceptorRegistry registry,
                                                                               final KinesisAsyncClient kinesisClient) {
        LOG.info("Auto-configuring Kinesis MessageSenderEndpointFactory");
        return new KinesisMessageSenderEndpointFactory(registry, kinesisClient);
    }

    @Bean
    @ConditionalOnMissingBean(name = "messageLogReceiverEndpointFactory")
    public MessageLogReceiverEndpointFactory messageLogReceiverEndpointFactory(final MessageInterceptorRegistry interceptorRegistry,
                                                                               final KinesisAsyncClient kinesisClient,
                                                                               final ApplicationEventPublisher eventPublisher) {
        LOG.info("Auto-configuring Kinesis MessageLogReceiverEndpointFactory");
        final ExecutorService executorService = newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("kinesis-message-log-%d").build()
        );
        return new KinesisMessageLogReceiverEndpointFactory(interceptorRegistry, kinesisClient, executorService, eventPublisher);
    }



    public static class DefaultLoggingRetryCondition implements RetryCondition {

        private final int warnCount;
        private final int errorCount;

        public DefaultLoggingRetryCondition(int warnCount, int errorCount) {
            this.warnCount = warnCount;
            this.errorCount = errorCount;
        }

        @Override
        public boolean shouldRetry(RetryPolicyContext context) {
            //noinspection unchecked
            boolean shouldRetry = OrRetryCondition.create(RetryCondition.defaultRetryCondition(), RetryOnExceptionsCondition.create(SdkClientException.class)).shouldRetry(context);
            logRetryAttempt(context, shouldRetry);
            return shouldRetry;
        }

        private void logRetryAttempt(RetryPolicyContext c, boolean shouldRetry) {
            final String operationName = c.executionAttributes().getAttribute(OPERATION_NAME);
            final String serviceName = c.executionAttributes().getAttribute(SERVICE_NAME);

            String message;
            if (c.exception() != null) {
                message = String.format("'%s' request to '%s' failed with exception on try %s: %s - %s - will retry: %s", operationName, serviceName, c.retriesAttempted(), c.exception().toString(), findExceptionMessage(c.exception()), shouldRetry);
            } else {
                message = String.format("'%s' request to '%s' failed without exception on try %s  - will retry: %s", operationName, serviceName, c.retriesAttempted(), shouldRetry);
            }

            if (c.retriesAttempted() >= errorCount) {
                LOG.error(message);
            } else if (c.retriesAttempted() >= warnCount) {
                LOG.warn(message);
            } else {
                LOG.info(message);
            }
        }

        private String findExceptionMessage(Throwable t) {
            if (t == null) {
                return null;
            }
            if (t.getMessage() != null) {
                return t.getMessage();
            }
            return findExceptionMessage(t.getCause());
        }
    }
}
