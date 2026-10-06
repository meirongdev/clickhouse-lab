#!/usr/bin/env bash
# 集群生命周期：./cluster.sh up [all] | down | status
#   up       只起 ClickHouse（1 keeper + 3 副本）。除 09、21、23 之外的实验都只要这个。
#   up all   再加 Kafka 栈（ZooKeeper + Kafka + Kafka Connect），实验 09、21、23 要用。
#            第一次会下载 clickhouse-kafka-connect 插件并校验 sha256，插件不进 git。
#   down     两套一起删，连数据卷。
set -euo pipefail
cd "$(dirname "$0")"
source ./lib.sh
source ./lib-kafka.sh

CH_ONLY=(-f docker-compose.yml)
WITH_KAFKA=(-f docker-compose.yml -f docker-compose-kafka.yml)

# 插件版本对齐 docs/mechanism-map.md 引的源码锚点 v1.3.9。
# sha256 和 GitHub release 页给这个 asset 公布的 digest 一致（2026-10-05 核过）。
PLUGIN_VER=v1.3.9
PLUGIN_SHA256=3a06719aacfa0ecd64e5a5b58c24c835e4c46895b322fb72f4bceafbcd255ee6
PLUGIN_URL=https://github.com/ClickHouse/clickhouse-kafka-connect/releases/download/$PLUGIN_VER/clickhouse-kafka-connect-$PLUGIN_VER.zip

sha256_of() { if command -v sha256sum >/dev/null; then sha256sum "$1"; else shasum -a 256 "$1"; fi | awk '{print $1}'; }

fetch_plugin() {
  local dir=connect-plugins/clickhouse-kafka-connect-$PLUGIN_VER zip
  [ -f "$dir/manifest.json" ] && return 0
  mkdir -p connect-plugins
  zip=connect-plugins/.download-$PLUGIN_VER.zip
  echo "下载 clickhouse-kafka-connect $PLUGIN_VER ..."
  curl -fsSL -o "$zip" "$PLUGIN_URL"
  if [ "$(sha256_of "$zip")" != "$PLUGIN_SHA256" ]; then
    rm -f "$zip"; echo "插件 sha256 对不上，不装" >&2; exit 1
  fi
  unzip -q -o "$zip" -d connect-plugins && rm -f "$zip"
}

wait_ch() {
  echo "等三个节点应答 ..."
  for i in $(seq 1 60); do
    n=$(q1 "SELECT count() FROM clusterAllReplicas('default', system.one)" 2>/dev/null || echo 0)
    [ "$n" = "3" ] && { echo "就绪：3 个副本，$(q1 "SELECT version()")"; return 0; }
    sleep 2
  done
  echo "超时，看 docker compose logs" >&2; exit 1
}

# Connect 起来要几十秒，而且 REST 先于插件扫描完成就开始应答，所以等的是「插件列表里有 sink」
wait_connect() {
  echo "等 Kafka Connect 加载插件 ..."
  for i in $(seq 1 90); do
    curl -sf -m 5 "$CONNECT/connector-plugins" 2>/dev/null | grep -q "$SINK_CLASS" && {
      echo "就绪：$(provenance_kafka | sed 's/^# //')"; return 0; }
    sleep 2
  done
  echo "Kafka Connect 超时，看 docker compose logs connect" >&2; exit 1
}

case "${1:-up}" in
  up)
    if [ "${2:-}" = "all" ]; then
      fetch_plugin
      docker compose "${WITH_KAFKA[@]}" up -d
      wait_ch; wait_connect
    else
      docker compose "${CH_ONLY[@]}" up -d
      wait_ch
    fi ;;
  down)   docker compose "${WITH_KAFKA[@]}" down -v ;;
  status) docker compose "${WITH_KAFKA[@]}" ps; q1 "SELECT version()" 2>/dev/null || true ;;
  *) echo "用法: $0 {up [all]|down|status}" >&2; exit 2 ;;
esac
