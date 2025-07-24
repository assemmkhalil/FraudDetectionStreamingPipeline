#!/usr/bin/env bash

while ! cqlsh cassandra -e "DESCRIBE CLUSTER" ; do
  echo "Waiting for Cassandra to be ready..."
  sleep 5
done

echo "init.cql"
cqlsh cassandra -f init.cql ;

echo "Initialization successfully completed."
