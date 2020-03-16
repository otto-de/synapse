# Release Notes

## 0.21.6
*synapse-edison*
* Fixes the StateRepositoryMetricsReporter in case the received message references a channel that is not created on startup

## 0.21.5
*synapse-edison*
* Add StateRepositoryMetricsReporter to log the size of state repositories

## 0.21.4
*synapse-aws-kinesis*
* Enable retry if SDKClientExceptions occur

## 0.21.3
*synapse-aws-kinesis*
* Add retry mechanism for expired Kinesis shard iterators

## 0.21.2
* Latest AWS SDK 2.10.56 as advised by AWS due to bugfixes

## 0.21.1
* Introduces FixedChannelHealthIndicator to measure health of asynchronously added eventsources 

## 0.21.0
*synapse-core*
* Fixes configuration of default ObjectMapper in that unknown properties are ignored during
  deserialization of messages.
* Adds new annotation @EnableMessageLogReceiverEndpoint to support annotation-driven configuration
  of MessageLogReceiverEndpoints.
* Adds new NitriteStateRepository, including support for secondary indexes.
* Introduces MessageSender interface
* Adds a simple TeeMessageSender that is sending messages to multiple delegate MessageSenders.

## 0.20.0
*synapse-core*
* add messageFormat attribute in @EnableMessageSenderEndpoint to be able to send meta data (e.g. versions)

## 0.19.1
*General*
* Latest AWS SDK 2.10.4 (which brings Jackson 2.10.0 and Netty 4.1.42.Final)
* Fix some degraded tests which broke through spring boot update

## 0.19.0
*General*
* Update to Edison 2.2.0, Spring Boot 2.2 and Spring 5.2

## 0.18.1
*General*
* Set compile target compatibility to Java 8

## 0.18.0
*Release accidentally built with Java 12. Please use version 0.18.1 instead.*

*General*
* Updates to Gradle 5.6.3

*synapse-aws-kinesis*
* Bugfix: Fallback to reading from horizon in case the initial shard iterator request leads to an exception

## 0.17.10
*General*
* Updates to AWS SDK 2.9.10
* Updates to Mockito 3.1.0

*synapse-core*
* ObjectMapper is now @ConditionalOnMissingBean

## 0.17.9
*General*
* Dependency updates

## 0.17.8
*synapse-edison*
* Log duration behind for every channel to micrometer when property `synapse.edison.metrics.enabled=true` 

## 0.17.7
*Bugfix*
* Improve exception handling in InMemoryChannel (to match other channels)

## 0.17.6
*Bugfix*
* Fix counter of messages in KinesisShardReader

## 0.17.5
*CompactionService*

* Enable marker support to some classes that are used during compaction

## 0.17.4
* Some more info logging during snapshot message dispatching

## 0.17.3
*StateRepository*

* Don't use on-heap keySet in ChronicleMapStateRepository

## 0.17.2
*StateRepository*

* You can specify your own custom ValueMarshaller for your ChronicleMapStateRepository.
  Add the valueMarshaller to the ChronicleMapBuilder AND specify .withCustomValueMarshaller(true) when building the
  ChronicleMapStateRepository.

*MessageStore*

* MessageStore has a isCompacting() property now which is used to dispatch messages from snapshots concurrently. This
  speeds up the processing of compacted snapshots at startup significantly.

*Bugfixes*
* Fixes the exception for an empty result of entries in the staterepository
* Fix re-using of already downloaded snapshots when having multiple channels (wildcard deletion of snapshots before
  downloading was too wild)

## 0.17.1

*Bugfixes*
* Fixes NPE for null payloads in message traces and journals

*Journaling, MessageTrace, StateRepository*
* Adds short help texts to Edison UIs

## 0.17.0

*Bugfixes*
* Messages that can not be translated and/or processed by consumers will not be deleted from
  SQS anymore, so that the message will be sent to the dead-letter queue after some time.

*MessageStore*
* Removes unused MessageStore.getName()

*Journaling*
* Introduces journaling for event-sourced entities stored in a StateRepository.
  Using message journals, it is possible to retrieve all messages that where used to modify
  the state of an event-sourced entity. Have a look at `de.otto.synapse.journal.Journal` for
  more information.

## 0.16.4

* Fixes bean instantiation of sqs retry policy.

## 0.16.3

* Adds a retry policy to sqs async client.

## 0.16.2

* Adds an edison status detail indicator to expose the number of entries in a state repository.

## 0.16.0

*StateRepository*

* Fixes design flaw of `MessageStore`: the stores can now contain messages from different channels.

* Removes WritableMessageStore interface and adds optional method `MessageStore.add()`

* Introduces required property `StateRepository.getName()`

* StatefulMessageConsumer now supports BiFunctions as arguments. This way, the consumer is able to modify existing
  entries in the `StateRepository` by applying the `BiFunction` to the existing state and the incoming message.
  
* Adds an UI and a REST API to access entities of a StateRepository in Edison microservices. 

*MessageStore*

* Adds possibility to add indexes to a `MessageStore` that can be used to filter messages by partitionKey, channel,
  hostname, etc. pp.
  
  By indexing the partition-key of the messages, it is possible to retrieve all messages that where changing the 
  state of a single entity.

*Leader Election*

* Implements a simple Leader Election using Redisson RLock

## 0.15.2
* Fixes deployment of Kinesis services by reducing the polling after Kinesis has been fully consumed 

## 0.15.1
* Fixes NPE in message traces if payload is null

## 0.15.0
* Introduces TextMessage that is used instead of Message<String>.
* Fixes possible threading issue in ObjectMappers.

## 0.14.5
* Updates to edison-microservice 2.0.0-rc3
* Fixes resource leak in CompactionService that was causing creation of additional threads with every compaction.
* Because of some changing default behaviour in Spring regarding overriding beans, your might discover 
  BeanDefinitionOverrideExceptions when running tests or your application. Consider setting property
  `spring.main.allow-bean-definition-overriding=true`. 

## 0.14.4
* Introduces consumeAll() in StateRepository 
* Sets keySet() in StateRepository to be deprecated as implementations did a full copy the map causing errors on bigger maps, use consumeAll instead

## 0.14.2
* Fixes broken links in message traces

## 0.14.1
* Adds blocking send + retries to Kinesis message senders in order to avoid rate-limit exceeded situations

## 0.14.0
* Updates to AWS SDK release version 2.2.0
* Updates to Spring Boot 2.0.4 + Edison Microservice 2.0 + Spring 5.0.8
* Refactors creation of Header objects:
    - Renames emptyHeader() to of()
    - Renames requestHeader(...) to of(...)
    - Renames responseHeader(...) to of(...)
* Fixes bug in Kinesis filterValues endpoints, that prevented registered MessageInterceptors to intercept received messages.
* Fixes usage of Kinesis 'approximateArrivalTimestamp'
* Fixes possible out-of-order arrival of messages if the message key is
  not suitable for selecting the same partition for all messages of a 
  single entity. For example, product-messages will get out of order,
  if the message type (price-update, availability-update) and so on is
  used to create a message key.
* Introduces message Key and refactors Message.getKey() etc. to use
  Key instead of String.

  Using Key, it is now possible to distinguish between a partitionKey used to
  select the channel partition (shard), and a  compactionKey used to do 
  the message compaction.
* Extends serialization format (MessageCodec) to add partitionKey and
  compactionKey into the message.
* Adds ordering of MessageInterceptors
* StateRepository is now an interface.
* Adds DelegatingStateRepository
* Adds EdisonStateRepository
* Refactors StateRepository to return Optionals consistently
   
## 0.13.0
* Adds ChannelResponse and ShardResponse to the core packages and introduces new possibility to stop message-consumption
  using predicates in MessageLogReceiverEndpoints.
* @EnableMessageSenderEndpoint does not use a default value for selectors anymore as this is prone to errors.
* Adds possibility to override the ObjectMapper used by Synapse
 
## 0.12.2
* Fix serialization error on polymorph payload objects

## 0.12.1
* Fixes + refactorings

## 0.12.0
* Introduces ObjectMappers.defaultObjectMapper() and removes usage of 
  ObjectMapper from Spring Boot, as this is not under control of Synapse 
  and might be configured in a way that prevents Synapse to parse messages
  generated by a different application
* Refactors registration of message interceptors: interceptors can now 
  only be registered at the MessageInterceptorRegistry, not directly at 
  an InterceptorChain anymore.
  
  This prevents bugs where interceptors are getting lost after registration like,
  for example, in the MessageTrace configuration.
* Adds possibility to send and receive custom headers for Kinesis channels. Introduces new transport-format
  for Kinesis messages: beside the old one (Kinesis paylpoad = message payload), a new format is added, where 
  custom message-header attributes are supported by adding format version and message headers to the Kinesis payload.
  
  While 0.12.0 receivers are still able to read from Synapse Kinesis streams generated by 0.11.0 senders, old
  0.11.0 receivers will not be able to read messages sent by 0.12.0 senders anymore.  
* Adds possibility to send and receive custom headers for SQS channels
* Introduces `DefaultSenderHeaderInterceptor` to automatically set some headers
  when sending messages:
    - message ID
    - sender name
    - sender timestamp
  
## 0.11.1
* Re-add method `purgeQueue`to `SqsClientHelper`
* add further exceptions to retry template when consuming

## 0.11.0
* Introduced new module `synapse-aws-auth` containing the configuration of an AwsCredentialsProvider.
* Introduced new module `synapse-compaction-aws-s3` for message compaction using AWS S3.
* Introduced new module `synapse-aws-sqs` for SQS message queues.
* Introduced new module `synapse-aws-kinesis` for Kinesis message logs.
* Updates from old synchronous KinesisClient to new KinesisAsyncClient.
* Adds `@MessageInterceptor` annotation used to easily intercept messages at sender and/or filterValues side of a channel. 
* Adds possibility to configure a RetryPolicy for Kinesis.
* Moved annotations etc. from `de.otto.synapse.annotation.messagequeue` to `de.otto.synapse.annotation`.
* Renames ChronicleMapStateRepository.chronicleMapConcurrentMapStateRepositoryBuilder() to .builder().
* Updates 3rd-party dependencies:
    - springVersion = "4.3.20.RELEASE"
    - springBootVersion = "1.5.17.RELEASE"
    - edisonVersion = "1.2.30"
* Adds new module 'synapse-redis' containing a preliminary version of a RedisMessageStore

## 0.10.0
* Updates to aws-java-sdk-preview-12
* Fixes naming of ```MessageQueueReceiverEndpoint``` beans using ```@EnableMessageQueueReceiverEndpoint``` annotations
* Separated configuration of event sources and message queues. In case of annotation-based configuration,
  nothing has to be changed. Only if no ```EnableEventSource``` annotation is used, the auto-configuration must
  now be triggered by adding the new ```EnableEventSourcing``` annotation to one of your Spring configurations.
* Added annotations-based configuration of ```MessageSenderEndpoints``` using the new annotation 
  ```EnableMessageSenderEndpoint```. Both Kinesis and SQS message senders are supported and automatically configured. 
* Removed dependency to ```de.otto.edison:edison-aws-s3```. The configuration has changed as follows:
  - **`aws.region`** renamed to `synapse.aws.region`
  - **`aws.profile`** renamed to `synapse.aws.profile`
* ```MessageSenderEndpoint.send(Message)``` and ```MessageSenderEndpoint.sendBatch(Stream<Message>)``` are now returning
  CompletableFuture<Void>.  

## 0.9.4
* Fixes startup of application context when eventsources are missing or not available

## 0.9.3
* Log times required to consume streams at startup   

## 0.9.0
* Adds annotation @EnableMessageQueueReceiverEndpoint to auto-configure SQS message-queue receivers
* Adds annotation @MessageQueueConsumer to auto-configure SQS message-queue listeners
* Adds message tracing for SQS queues to sender- and filterValues-endpoints
* Adds combined message trace for all sender and filterValues channels
* Introduces synapse-testsupport with separate in-memory configurations for message logs and message queues

## 0.8.0
**Breaking Change**: Beans need to be qualified.
* Register two MessageSenderEndpointFactories: 
  * kinesisMessageSenderEndpointFactory for Kinesis
  * sqsMessageSenderEndpointFactory for SQS

## 0.7.3
* Fix bug in KinesisShardIterator. Read next record when there is no data near the part of the shard pointed to by the ShardIterator. See https://docs.aws.amazon.com/streams/latest/dev/troubleshooting-consumers.html#getrecords-returns-empty.

## 0.7.2
* Fix name for InMemoryMessageSenderFactory in InMemoryTestConfiguration to override kinesisSenderEndpointFactory in autowiring 

## 0.7.1
* Update dependency to edison-aws 0.4.1 which works with AWS SDK preview 10

## 0.7.0
* Updated to AWS SDK preview 10
* Added support for AWS SQS with SqsMessageSender and SqsMessageQueueReceiverEndpoint
* New KinesisMessageLogReader for low-level polling of Kinesis messages.
* Refactored interfaces for `EventSource` and `MessageLogReceiverEndpoint`. The interfaces now immediately return a 
CompletedFuture instead of blocking forever.
* Add StartFrom AT-POSITION to access entries in the stream directly

## 0.6.13
* Add ConditionalOnMissingBean to ObjectMapper in SynapseAutoConfiguration

## 0.6.12
* Fixes bug that don't create a new message when retry with a corrupt byte buffer 

## 0.6.11
* Adds message traces for sender- and filterValues endpoints to Edison µServices. 

## 0.6.9
* Fixes bug that SnapshotAutoConfiguration is not injecting the ApplicationEventPublisher into SnapshotMessageStore
  instances created by the SnapshotMessageStoreFactory.
* Log message meta data when put to kinesis failed

## 0.6.8
* Fixes bug in MessageReceiverEndpointInfoProvider resulting in a broken presentation of status details. 
* Disabling of synapse-edison is now more consistent. The different Health Indicators can now be disabled using the
  following properties:
  - `StartUpHealthIndicator`: synapse.edison.health.startup.enabled=false
  - `SnapshotReaderHealthIndicator`: synapse.edison.health.snapshotreader.enabled=false
  - `MessageReceiverEndpointHealthIndicator`: synapse.edison.health.messagereceiver.enabled=false
  
## 0.6.7
* Introduced interfaces for MessageEndpoint, MessageReceiverEndpoint, MessageLogReceiverEndpoint and 
  MessageQueueReceiverEndpoints.
* Refactored the creation of EventSources: The associated MessageLogReceiverEndpoints are now registered in the 
  ApplicationContext, so it is possible to inject these into other beans.
  
## 0.6.6
* Removed `EnableEventSource#builder()` and replaced it by an 
  auto-configuration of the new `MessageSenderEndpointFactory` and 
  `MessageLogReceiverEndpointFactory` instances, together with the (also new) 
  general-purpose `DefaultEventSource` implementation that is replacing the
  different other `EventSource` implementations.
* Simplified the configuration of in-memory implementations of the different endpoints for
  testing purposes. It is now possible to just add `@ImportAutoConfiguration(InMemoryTestConfiguration.class)` to
  your test configuration to do this.
* Removed `Predicate` from `EventSource` and `MessageLogReceiverEndpoint` interfaces and
  replaced it by `consumeUntil()` methods taking an `Instant`as a parameter to stop
  message retrieval at a specified timestamp.
* Removed `durationBehind` from channel- and shard-positions.
* Introduced type `ChannelDurationBehind` that is used in notifications to announce the duration that consumers are 
  behind of the channel head position.   
* Renamed `EventSourcingHealthIndicator` to `MessageReceiverEndpointHealthIndicator`
* Introduced `SnapshotReaderHealthIndicator`  
* Renamed `EventSourcingStatusDetailIndicator` to `MessageReceiverStatusDetailIndicator`
* Introduced `SnapshotStatusDetailIndicator`
* Refactored eventsource notifications and separated them into `SnapshotReaderNotification` and 
  `MessageReceiverNotification`.

## 0.6.5
* Added `StartupHealthIndicator` that is unhealthy until all EventSources are (almost) up to date.
* Added possibility to consume MessageLogs from timestamp

## 0.6.4
* Fixed problem that the KinesisShardIterator will not recover after an exception is thrown
* Introduced MessageEndpointConfigurer used to register MessageInterceptors at MessageSender- and/or
  MessageReceiverEndpoints.
* Added MessageFilter as a special implementation of a MessageInterceptor that is used to filter messages depending
  on a Predicate

## 0.6.3
* Using key-value pairs in (some) log messages

## 0.6.2
* Speedup snapshot creation and log progress

## 0.6.1
* Introduce special SnapshotEventSourceNotification that additionally holds the timestamp of snapshot creation.

## 0.5.0 Major Refactoring
* Renamed project to OTTO Synapse:
  * eventsourcing-core -> synapse-core
  * eventsourcing-aws -> synapse-aws
  * eventsourcing-edison-integration -> synapse-edison
* Renamed packages to de.otto.synapse.*
* Renamed properties to synapse.*
* Introduced eventsourcing-aws and removed aws-specific parts from eventsourcing-/synapse-core
* Renamed Event to Message and removed EventBody

## 0.4.8
* **[eventsourcing-edison-integration]** Add health indicator for stream state.
 This means that a service goes unhealthy if a stream is in a not-recoverable state.
* **[eventsourcing-core]** Fix retry policy in `KinesisShardIterator` to retry
 also on connection errors, not only on throughput exceed errors. 

## 0.4.7
Remove `Clock` bean as this is required only for tests.
Don't set StatusDetailIndicator to warn when kinesis consumer has finished.

## 0.4.6
Add `Clock` bean that is required by `EventSourcingStatusDetailIndicator` 

## 0.4.5
* New sub project `eventsourcing-edison-integration`. This project contains a `StatusDetailIndicator` that provides 
StatusDetail information for each EventSource.

## 0.4.4
* Also publish EventSourceNotification Application Events for Kinesis and InMemory EventSources

## 0.4.3
* Remove further ChronicleMap closed errors on shutdown. 

## 0.4.2
* Prevent ChronicleMap closed errors on shutdown.

## 0.4.1
* Add `sendEvents` method to `EventSender` interface.

## 0.4.0
* Breaking changes:
  * Change signature of `KinesisEventSender.sendEvents`. Send events takes a list of `EventBody` now
  * Split Event class into `Event` and `EventBody`
  * `CompactionService` now requires a `StateRepository` with name `compactionStateRepository` 
* InMemory EventSender and EventSource for testing


## 0.3.0
* Events with "null"-payload will delete the entry.
* Remove client side encryption because kinesis now supports server side encryption.

## 0.2.2
* `SnapshotReadService` now allows to set a local snapshot file to read from instead retrieving it from AWS S3.   
This functionality was moved from `SnapshotEventSource` and also works for a `CompactingKinesisEventSource` now.

## 0.2.1
* Provide `EncryptedOrPlainJsonTextEncryptor` that checks whether data is plain json or is encrypted.

## 0.2.0
* Add option to send unencrypted events to `KinesisEventSender.sendEvent(String, Object, boolean)` 
and `KinesisEventSender.sendEvents(Map<String,Object>, boolean)`

## 0.1.11
* Fix statistics
* Clear state repository after compaction job

## 0.1.1
* Released to keep things stable

## 0.1.0
**Initial Release**
