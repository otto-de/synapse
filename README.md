# OTTO Synapse
A library to implement event-sourcing microservices.

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/de.otto.synapse/synapse-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/de.otto.synapse/synapse-core)

# Setup for Development

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
