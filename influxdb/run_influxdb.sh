docker pull influxdb
docker run -d -p 8086:8086 influxdb


docker run --rm -e INFLUXDB_HTTP_AUTH_ENABLED=true \
         -e INFLUXDB_ADMIN_USER=admin \
         -e INFLUXDB_ADMIN_PASSWORD=admin \
         -v /var/lib/influxdb:/var/lib/influxdb \
         -v /etc/influxdb/scripts:/docker-entrypoint-initdb.d \
         influxdb /init-influxdb.sh