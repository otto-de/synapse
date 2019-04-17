# *User Guide*

Synapse is a library used to implement eventsourcing for Spring Boot applications. 

# Message Passing

In a distributed system, service must communicate with each other. In many cases, communication is implemented using
request-response models like HTTP. Message passing is another option: tools like ActiveMQ, RabbitMQ, Kafka or Kinesis 
provide an infrastructure, to build such kind of message-passing architectures.

No matter, which solution you are using, you must decide which kind of message you want to send:
 
## Message

    `de.otto.synapse.message.Message`

A `Message` is the basic unit of communication in a message-passing system. The most simple message will only carry 
some payload like, for example, an ID, an object, a command or event. Messages have no special intent.

Having no special intent makes messages generic, but also less meaningful. That's why we can use two more concepts on
top of messages.

In Synapse, messages are represented using `de.otto.synapse.message.Message`
 
See [EIP: Message](https://www.enterpriseintegrationpatterns.com/patterns/messaging/Message.html)

## Event

    There is currently no special representation in Synapse. Use `de.otto.synapse.message.Message` instead.

An event IS-A message which informs various listeners about something which has already happened.

![EIP Event Message](https://www.enterpriseintegrationpatterns.com/img/EventMessageSolution.gif)

Events are sent by [message senders](#MessageSender) which doesn't know (and don't case) about the 
[consumers](#MessageConsumer) of the event. 

A typical example would be an online shop. Whenever an order is placed, the shop would publish an 
`OrderSubmittedEvent` to inform other systems (e.g. the logistics system) about the new order. However, the shop 
doesn’t care – or even know – about the consumers. If the logistics system isn’t interested in the events 
anymore, it just unsubscribes from the list of consumers.

Because multiple consumers might be interested in receiving events, topics or [message logs](#MessageLog) should be 
used to transmit events (one-to-many).

See [EIP: Event Message](https://www.enterpriseintegrationpatterns.com/patterns/messaging/EventMessage.html)

## Command

    There is currently no special representation in Synapse. Use `de.otto.synapse.message.Message` instead.

A command IS-A message that carries the request to execute some action. Most of the time, commands will be transferred
using point-to-point connections aka [message queues](#MessageQueue).

![EIP: Command Message](https://www.enterpriseintegrationpatterns.com/img/CommandMessageSolution.gif)

In case of an online shop, one such example could be the `BillCustomerCommand`. After an order is placed, the online
sop sends this command to the billing system to trigger the invoice.

The difference between messages, events and commands lies in their intent. While messages have no special intent at all,
events inform about something which has happened and is already completed (in the past). Commands trigger something
which should happen (in the future).
   
See [EIP: Command Message](https://www.enterpriseintegrationpatterns.com/patterns/messaging/CommandMessage.html)

## Key

    `de.otto.synapse.message.Key`

Every Synapse `Message` has a `Key` that is especially used for partitioning and compaction purposes. A key is either
simple key (`de.synapse.message.SimpleKey`) containing something like an entity-id, or a compound key 
(`de.synapse.message.CompoundKey`) consisting of a partition-key and a compaction-key (see section 
[Log Compaction](#Log_Compaction) for details about this). 

````java
final Key simpleKey = Key.of("urn:product:42");
final Key compoundKey = Key.of("urn:product:42", "urn:product:price:42");
````

In most situations, the `Key#partitionKey()` of a message should be the primary key or entity-id of the entity, that
is addressed by the message. For example, a `ProductUpdated` event should use the `product id` of the updated product
as partition key for the message. This way, all the messages concerning a single product will be transmitted in order,
even if the transport channel is configured with several partitions.

The `Key#compactionKey()` could be something different. For example, product updates might need more than a single 
kind of message: `ProductDataUpdated`, `ProductPriceUpdated` and `ProductAvailabilityUpdated`. All three messages should
be transferred using a single channel, and for all three messages, the partition key should be the product-id. In order
to keep the latest messages for every product, the compaction key could be something like `<product-id>#DATA`, 
`<product-id>#PRICE` and `<product-id>#AVAILABILITY`. Now, all messages will be sent in order, also in partitioned
channel configurations, and message-log compaction will keep the latest messages for all three kinds of events.

Please not, however, that...
* ...different messaging systems might have restrictions regarding the length of message keys. 
* ...you might need to later change message keys because of infrastructural changes or other changes in the message 
handling. Changes in the numbers of partitions, introduction of message compaction, etc. pp.  

## Header

    `de.otto.synapse.message.Header`

Synapse is supporting a number of default headers that are defined in `de.synapse.message.DefaultHeaderAttr`.

* `synapse_msg_id`: Unique identifier of the message.
* `synapse_msg_sender`: The name of the message-sender.
* `synapse_msg_arrival_ts`: The header attribute containing the timestamp to which the message has arrived in Kinesis. 
    Other channel implementations may use this attribute for similar timestamps, as long as the semantics of the property 
    is the met.
* `synapse_msg_sender_ts`: The timestamp to which the message was sent.
* `synapse_msg_receiver_ts`: The timestamp to which the message was received by a [MessageReceiverEndpoint](#MessageReceiverEndpoint).

Example: Reading headers
```java
void foo(final Message msg) {

    final String messageId = msg
            .getHeader()
            .getAsString(DefaultHeaderAttr.MSG_ID);
    final Instant senderTs = msg
            .getHeader()
            .getAsInstant(DefaultHeaderAttr.MSG_SENDER_TS);

    ...        
}
```

See [DefaultSenderHeadersInterceptor](#DefaultSenderHeadersInterceptor) and [DefaultReceiverHeadersInterceptor](#DefaultReceiverHeadersInterceptor) for details about how and when 
these headers will be available.
 
## Payload

## MessageFormat & MessageCodec

# Channels

## Endpoints

### MessageReceiverEndpoint

### MessageQueueReceiverEndpoint

### MessageLogReceiverEndpoint

## MessageInterceptor

### DefaultSenderHeadersInterceptor

### DefaultReceiverHeadersInterceptor

## MessageSender

## MessageLog

### Log Compaction

## MessageQueue

## MessageConsumer

## StateRepository

A StateRepository is a repository that is holding the current state of event-sourced entities. For example,
a ProductStateRepository might contain Product entities, with some productId beeing the entityId used in 
the StateRepository as a key to look up products.

Synapse provides a number of default implementations for the StateRepository interface:
* *ConcurrentMapStateRepository*: A StateRepository that is using a configurable ConcurrentMap to store entities
* *ChronicleMapStateRepository*: A ConcurrentMapStateRepository using a ChronicleMap for large amounts of data stored
  outside off the heap. 

### StateRepository UI

The optional synapse-edison library contains an auto-configured UI for Edison microservices. For every StateRepository
in the application context, a link to a page is registered in the /internal navigation. The UI renders a representation
of all entities in the StateRepository using Jackson's ObjectMapper. Therefore, entities should be able to be 
transformed into a proper JSON representation, otherwise the UI will not be very helpful.

Registration of StateRepository UIs can be globally disabled by setting `synapse.edison.state.ui.enabled = false`.

Single repositories can be excluded, too: `synapse.edison.state.ui.excluded = FirstStateRepository,SecondStateRepository`
or, using Yaml configuration files,
````yaml
synapse:
  edison:
    state:
      ui:
        enabled: true
        excluded:
          - FirstStateRepository
          - SecondStateRepository    
```` 

Beside of the UI, Synapse-Edison also offers a REST-API to access the entities of the StateRepositories. The API 
uses `application/hal+json` as media type and is accessible by default under `/internal/staterepositories`.

## MessageStore

    `de.otto.synapse.messagestore.MessageStore'

A MessageStore is a repository that is storing messages, while keeping the insertion-order of the messages. 

![Message Store](http://www.enterpriseintegrationpatterns.com/img/MessageStore.gif)

The `MessageStore` interface supports streaming access to the stored messages. The implementation are expected to 
support large amounts of messages, so returning collections instead of streams is not appropriate.

Messages can be originated in different [channels](#Channels). `MessageStore.stream(channelName)` provides access
to the messages for a single stream. Using `MessageStore.getChannelNames()`, the set of channel-names known by the
store can be accessed. 

Synapse `MessageStore` also provides access to the latest `ChannelPosition` for every known channel name. The position 
is derived from the (optional) message-[header](#Header)'s `ShardPosition`, so it will only be usable for messages
originated in a  [MessageLog](#MessageLog). 

If the `MessageStore` is used as a store for message snapshots, the `ChannelPosition` can be used to determine the 
starting position in a `MessageLog`. 

In most cases, messages can be added to Message Stores. Only a few special-purpose implementation do not support 
adding messages to the store. 
    
There are several implementations of the `MessageStore` interface available in Synapse:
* `de.otto.synapse.messagestore.OnHeapIndexingMessageStore`: A `MessageStore` that is implemented using a 
  `ConcurrentLinkedDeque` with support for message-indexing. Primarily used for testing purposes.
* `de.otto.synapse.messagestore.OnHeapRingBufferMessageStore`: A `MessageStore` that is implemented using a 
  Guava `EvictingQueue`. Primarily used for testing purposes, or for smaller data-sets without any need for durability.
* `de.otto.synapse.messagestore.OnHeapCompactingMessageStore`: A compacting `MessageStore` that is compacting messages
  by compaction-key when writing into the store. It's implementation is based on a `ConcurrentLinkedHashMap` in order
  to preserve the insertion-order of messages.

For larger datasets, Redis is supported by the separate, add-on library `synapse-redis`. It can also be used to
access AWS ElastiCache. 
* `de.otto.synapse.messagestore.redis.RedisRingBufferMessageStore`: a Redis implementation of the `MessageStore` 
  interface that can be configured with a maximum number of messages. The latest N messages will be stored, so the
  implementation is behaving like a fixed-sized ring buffer.
* `de.otto.synapse.messagestore.redis.RedisIndexedMessageStore`: another Redis implementation, that is using
  both maximum size + eviction to prevent the `MessageStore` to increase in size without any bounds. This implementation
  is also offering a single index that is by default indexing the message's partition key. Using this index, it is
  possible to retrieve all messages for a given partition-key, which in most cases is equivalent to all the messages 
  that where changing a single given entity.

See [EIP: Message Store](http://www.enterpriseintegrationpatterns.com/patterns/messaging/MessageStore.html) 

### Indexed Access

The `MessageStore` interface is supporting basic indexing of stored entries. For example, you could add an `Index` for
the `partitionKey` of the messages, so you can later fetch the ordered list of messages for a given key:

```java
@Configuration
public class TestConfiguration {
    @Bean
    public MessageStore messageStore(final RedisTemplate<String, String> redisTemplate) {
    
        return new RedisIndexedMessageStore(
                "test-store",
                100 /* batch size */, 
                10000 /* max. number of entries */,
                7*24*60*60*1000 /* evict entries after 7 days */,
                Indexers.partitionKeyIndexer(),
                redisTemplate);
    }
}
```

In order to retrieve all the messages for some partitionKey, `MessageStore.stream(Index,String)` can be used:

```java
@Component
public class Foo {
    private final MessageStore messageStore;
    
    public Foo(final MessageStore messageStore) {
        this.messageStore = messageStore;
    }
    
    public void printAllMessageFor(final String partitionKey) {
        messageStore
                .stream(Index.PARTITION_KEY, partitionKey)
                .forEach(System.out::println);
    }
}
``` 

The current indexing capabilities are rather limited. While it is already supported to add several indexes, it is only 
possible to stream the messages for a single index. However, you can stream the messages for an index, and use 
`Stream.filter` to further reduce the stream using filter values. To do this, `MessageStoreEntry.getFilterValues()` 
contains the filter values for all calculated indexes.

To create multiple indexes, `Indexers.compositeIndexer()` can be used to create a composite from two or more `Indexer`
implementations:

```java
final Indexer indexer = Indexers.composite(
        Indexers.partitionKeyIndexer(),
        Indexers.originIndexer("Snapshot"),
        Indexers.serviceInstanceIndexer("my-service@localhost:8080")
);
```
If the RedisIndexedMessageStore (or some other implementation supporting indexes) is created with this indexer,
for every message added to the store, all three indexes will be updated. Now we can retrieve all messages that 
originated in the snapshot that where added to the store by my-service:

```java
messageStore
        .stream(Index.ORIGIN, "Snapshot")
        .filter(entry -> entry
                .getFilterValues()
                .getOrDefault(Index.SERVICE_INSTANCE, "")
                .startsWith("my-service"))
        .forEach(System.out::println);
```

# EventSource

