#!/usr/bin/env bash
# 集群生命周期：./cluster.sh up [all] [prod] | down | status
#   up       只起 ClickHouse（1 keeper + 3 副本）。除 09、21、23 之外的实验都只要这个。
#   up all   再加 Kafka 栈（ZooKeeper + Kafka + Kafka Connect），实验 09、21、23 要用。
#            第一次会下载 clickhouse-kafka-connect 插件并校验 sha256，插件不进 git。
#   up prod  再挂上 cfg/prod/ 的生产设置（Aiven 改过的服务端设置和 default profile），档位 TIER=A|B|C，默认 A。
#            可以和 all 一起写。实验 01–27 和 results/ 都跑在上游默认设置上，别带 prod 跑 run-all；
#            带上之后用 prod-check.sh 核对。切回默认设置直接 ./cluster.sh up，compose 会按新配置重建容器。
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

# check_prod  三个副本都带上了当前档位的生产设置：服务端取一项随档位变的、一项不变的，profile 取一项。
# 逐项、带行为的核对在 prod-check.sh。
check_prod() {
  local want n got bad=0
  want="$(xml_val max_concurrent_queries "cfg/prod/server-$TIER.xml")/0/$(xml_val max_concurrent_queries_for_all_users "cfg/prod/users-$TIER.xml")"
  for n in "${!NODES[@]}"; do
    got=$(q "${NODES[$n]}" "SELECT (SELECT value FROM system.server_settings WHERE name = 'max_concurrent_queries')
             || '/' || (SELECT value FROM system.server_settings WHERE name = 'database_atomic_delay_before_drop_table_sec')
             || '/' || toString(getSetting('max_concurrent_queries_for_all_users'))")
    [ "$got" = "$want" ] || { echo "${NODE_NAMES[$n]} 的生产设置没生效：${got}，期望 ${want}" >&2; bad=1; }
  done
  [ "$bad" = 0 ] || exit 1
  echo "生产设置已生效：档 ${TIER}（并发上限 / DROP 宽限期 / profile 并发上限 = ${want}，三个副本一致）。细项跑 bash prod-check.sh"
}

case "${1:-up}" in
  up)
    [ $# -gt 0 ] && shift
    files=("${CH_ONLY[@]}"); kafka=0; prod=0
    for a in "$@"; do
      case $a in
        all)  kafka=1 ;;
        prod) prod=1 ;;
        *) echo "用法: $0 {up [all] [prod]|down|status}" >&2; exit 2 ;;
      esac
    done
    if [ "$kafka" = 1 ]; then fetch_plugin; files+=(-f docker-compose-kafka.yml); fi
    if [ "$prod" = 1 ]; then
      export TIER=${TIER:-A}
      case $TIER in A|B|C) ;; *) echo "TIER 只能是 A、B、C，现在是 '$TIER'" >&2; exit 2 ;; esac
      files+=(-f docker-compose.prod.yml)
    fi
    docker compose "${files[@]}" up -d
    wait_ch
    if [ "$kafka" = 1 ]; then wait_connect; fi
    if [ "$prod" = 1 ]; then check_prod; fi ;;
  down)   docker compose "${WITH_KAFKA[@]}" down -v ;;
  status)
    docker compose "${WITH_KAFKA[@]}" ps
    if v=$(q1 "SELECT version()" 2>/dev/null) && [ -n "$v" ]; then
      t=$(prod_tier); echo "${v}，${t:+生产设置 档 $t}${t:-上游默认设置}"
    fi ;;
  *) echo "用法: $0 {up [all] [prod]|down|status}" >&2; exit 2 ;;
esac
