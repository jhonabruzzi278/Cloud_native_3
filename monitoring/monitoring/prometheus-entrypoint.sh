#!/bin/sh
set -eu

: "${KAFKA_1_PRIVATE_IP:?define KAFKA_1_PRIVATE_IP}"
: "${KAFKA_2_PRIVATE_IP:?define KAFKA_2_PRIVATE_IP}"
: "${ZOOKEEPER_PRIVATE_IP:?define ZOOKEEPER_PRIVATE_IP}"
: "${APP_PRIVATE_IP:?define APP_PRIVATE_IP}"

sed \
  -e "s|\${KAFKA_1_PRIVATE_IP}|${KAFKA_1_PRIVATE_IP}|g" \
  -e "s|\${KAFKA_2_PRIVATE_IP}|${KAFKA_2_PRIVATE_IP}|g" \
  -e "s|\${ZOOKEEPER_PRIVATE_IP}|${ZOOKEEPER_PRIVATE_IP}|g" \
  -e "s|\${APP_PRIVATE_IP}|${APP_PRIVATE_IP}|g" \
  /etc/prometheus/prometheus.yml > /tmp/prometheus.yml

if grep -q '\${[A-Z_]*}' /tmp/prometheus.yml; then
  echo "Prometheus config still contains an unresolved target variable" >&2
  exit 1
fi

exec /bin/prometheus \
  --config.file=/tmp/prometheus.yml \
  "$@"
