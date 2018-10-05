## Overview

Distributed File Storage System with two-phase commit. The file server cluster is always available as long as over half of the nodes are up. Crashed nodes will catch up as soon as they're restarted.

The project is based on a starter code provided by Prof. George Porter

## To build the protocol buffer IDL into auto-generated stubs:

$ mvn protobuf:compile protobuf:compile-custom

## To build the code:

$ mvn package

## To run the services:

$ target/surfstore/bin/runBlockServer
$ target/surfstore/bin/runMetadataStore

## To run the client

$ target/surfstore/bin/runClient

## To delete all programs and object files

$ mvn clean
