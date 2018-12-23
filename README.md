# OTTO Synapse

Eventsourcing & message passing for Spring Boot microservices.


[![Maven Central](https://maven-badges.herokuapp.com/maven-central/de.otto.synapse/synapse-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/de.otto.synapse/synapse-core)

# User Guide

In most cases, microservices today make use of synchronous communication using RESTful APIs. This approach has
many advantages: most notably, developers will already know how to implement such kind of systems.

Especially in larger systems consisting of, say: dozens or hundreds of microservices, REST APIs do not feel appropriate
anymore. The smaller the services get, the more overhead is needed to persist data, cache data, implement, document and 
test the APIs, and so on. 

Eventsourcing is a radically different approach to implement microservices: 

> "Capture all changes to an application state as a sequence of events" ([Martin Fowler](https://www.martinfowler.com/eaaDev/EventSourcing.html))

* Services communicate between each other by exchanging asynchronous messages over messaging channels. 
* Messages might be "commands": a command to do something (in the future) like, for example, "Place Order"
* More often, messages will contain "events": the information that something has happened in the past like, for example, 
  "Product Deleted".
* Services are constantly reading and processing messages from messaging channels.  
* On startup, a service will read all messages, from the oldest to the newest one, until the internal state of the 
  service is up-to-date. Because doing so would take increasingly longer over time, regularly taken snapshots containing a 
  compacted list of messages is used. 
  
Synapse is a library implemented at otto to make it easy to create eventsourcing Spring Boot microservices. Annotations
are used to make it as simple as possible to use Synapse. 

Today, Synapse is supporting AWS SQS, Kinesis and Redis (as a message store). However, it should be easy to add
implementations for other infrastructures like, for example Kafka.   

## Message

> Enterprise Integration Patterns: [Message](http://www.enterpriseintegrationpatterns.com/patterns/messaging/Message.html)

![EIP: Message](http://www.enterpriseintegrationpatterns.com/img/MessageSolution.gif)


## Message Logs
## Message Queues
## Sending Messages
## Receiving Messages
## Intercepting Messages
## State Repositories
## Message Stores
## Compaction
## EventSource

1. Install Docker and LocalStack

Docker is required to run LocalStack, so first you have to [install Docker](https://docs.docker.com/install/).

LocalStack is "A fully functional local AWS cloud stack". Synapse is using LocalStack in order to run integration tests
locally, without having to access the "real" AWS services.

Follow the instructions here: https://github.com/localstack/localstack

2. Building Synapse

```
./gradlew startLocalStack
./gradlew build
./gradlew stopLocalStack
```
