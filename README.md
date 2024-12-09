# OTTO Synapse

Eventsourcing & message passing for Spring Boot microservices.

[![Build](https://github.com/otto-de/synapse/workflows/Build/badge.svg)](https://github.com/otto-de/synapse/actions?query=workflow%3ABuild)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/de.otto.synapse/synapse-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/de.otto.synapse/synapse-core)
![OSS Lifecycle](https://img.shields.io/osslifecycle?file_url=https%3A%2F%2Fraw.githubusercontent.com%2Fotto-de%2Fsynpase%2Fmain%2FOSSMETADATA)


## Building Synapse
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
