# 实验 28 的辅助工具：读或改停着的 ZooKeeper 快照里某个 znode 的 cversion（见 ZkCversion.java）。
# 编译用 JDK 镜像（ZooKeeper 镜像只带 JRE，没有 jdk.compiler）；运行用和 docker-compose-zk.yml 同一个
# ZooKeeper 镜像，读写快照的是同一份 3.8.4 的 jar、同一个 JRE。两个镜像都钉 tag + digest。
FROM eclipse-temurin:17-jdk@sha256:420933bf319e6aff4bec22eff8d2a31c824df5d853432e3dc0f46d6ea8dd5919 AS build
COPY --from=zookeeper:3.8.4@sha256:20d3c93ebda093d0c90911493396975e0e04523af225d8053d42bab781f9ca48 /apache-zookeeper-3.8.4-bin/lib /zk/lib
COPY ZkCversion.java logback.xml /src/
RUN javac -encoding UTF-8 -cp '/zk/lib/*' -d /out /src/ZkCversion.java && cp /src/logback.xml /out/

FROM zookeeper:3.8.4@sha256:20d3c93ebda093d0c90911493396975e0e04523af225d8053d42bab781f9ca48
COPY --from=build /out /tool
# 和 zk 容器里的进程同一个用户，写出来的新快照 ZooKeeper 读写得动
USER zookeeper
ENTRYPOINT ["java", "-Dzookeeper.digest.enabled=false", "-cp", "/tool:/apache-zookeeper-3.8.4-bin/lib/*", "ZkCversion"]
