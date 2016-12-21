#!/bin/bash

mkdir ./influxdb-data
mkdir ./grafana-data

docker run -h influxdb \
    -p 8083:8083 -p 8086:8086 \
    -v influxdb-data:/var/lib/influxdb \
    influxdb

docker run -h grafana \
    -i -p 3000:3000 \
    -v grafana-data:/var/lib/grafana \
    grafana/grafana

