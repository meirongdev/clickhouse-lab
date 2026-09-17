#!/usr/bin/env bash
# 集群生命周期：./cluster.sh up | down | status
set -euo pipefail
cd "$(dirname "$0")"
source ./lib.sh

case "${1:-up}" in
  up)
    docker compose up -d
    echo "等三个节点应答 ..."
    for i in $(seq 1 60); do
      n=$(q1 "SELECT count() FROM clusterAllReplicas('default', system.one)" 2>/dev/null || echo 0)
      [ "$n" = "3" ] && { echo "就绪：3 个副本"; q1 "SELECT version()"; exit 0; }
      sleep 2
    done
    echo "超时，看 docker compose logs" >&2; exit 1 ;;
  down)   docker compose down -v ;;
  status) docker compose ps; q1 "SELECT version()" 2>/dev/null || true ;;
  *) echo "用法: $0 {up|down|status}" >&2; exit 2 ;;
esac
