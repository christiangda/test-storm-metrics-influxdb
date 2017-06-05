#  test-storm-metrics-influxdb






## Run InfluxDB Docker container

```
mkdir -p ~/tmp/{influxdb,grafana} 

docker run -p 8083:8083 -p 8086:8086 \
      -v ~/tmp/influxdb:/var/lib/influxdb \
      influxdb
      
docker run -h grafana \
    -i -p 3000:3000 \
    -v ~/tmp/grafana:/var/lib/grafana \
    grafana/grafana

```