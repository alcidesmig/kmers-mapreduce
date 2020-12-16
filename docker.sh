#!/bin/sh
docker run --name hadoop_ -it --rm -p 50070:50070 -v /home/alcides/pde:/tmp/alcides sequenceiq/hadoop-docker:2.7.1 /etc/bootstrap.sh -bash
