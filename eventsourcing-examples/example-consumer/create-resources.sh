#!/usr/bin/env bash
aws kinesis create-stream --stream-name edison-example-bananas --shard-count 2 --endpoint http://localhost:4567
aws kinesis create-stream --stream-name edison-example-products --shard-count 2 --endpoint http://localhost:4567
