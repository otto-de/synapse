#!/usr/bin/env bash
aws kinesis create-stream --stream-name synapse-example-bananas --shard-count 2 --endpoint http://localhost:4567
aws kinesis create-stream --stream-name synapse-example-products --shard-count 2 --endpoint http://localhost:4567
