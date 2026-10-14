// 读或改 ZooKeeper 快照里某个 znode 的 cversion，实验 28 用。只能对停着的 ZooKeeper 用。
//
// 做法是用 ZooKeeper 3.8.4 自己的 FileTxnSnapLog 把快照和事务日志恢复成内存里的 DataTree，改掉目标节点的
// cversion，再存一份新快照。新快照的文件名是最后一个 zxid，比已有的快照新，ZooKeeper 启动时优先加载它。
// 树里除了这一个计数器，别的原样保留（会话表也一起存回去）。
//
// cversion 是父节点上的计数器：每建一个子节点加 1，顺序节点的序号就取它的当前值（PrepRequestProcessor），
// 删子节点不改它。把它改大，等于跳过了那么多次建子节点，用来在 lab 里把一张表的 log/ 推到 int32 上限附近。
//
// 用法：ZkCversion <事务日志目录> <快照目录> <znode 路径> [新的 cversion]
//       不给新值就只打印当前的 cversion 和子节点数，不写任何东西。
// 新快照不带 digest：JVM 参数 -Dzookeeper.digest.enabled=false（见 Dockerfile）。直接改 stat 不会更新
// DataTree 的增量 digest，带 digest 存下去，启动时重算出来对不上，会报 digest mismatch。
import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.zookeeper.server.DataNode;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;

public class ZkCversion {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("用法: ZkCversion <事务日志目录> <快照目录> <znode 路径> [新的 cversion]");
            System.exit(2);
        }
        FileTxnSnapLog snapLog = new FileTxnSnapLog(new File(args[0]), new File(args[1]));
        DataTree tree = new DataTree();
        ConcurrentHashMap<Long, Integer> sessions = new ConcurrentHashMap<>();
        long zxid = snapLog.restore(tree, sessions, (hdr, rec, digest) -> { });
        DataNode node = tree.getNode(args[2]);
        if (node == null) {
            System.err.println("快照里没有这个 znode: " + args[2]);
            System.exit(1);
        }
        int before = node.stat.getCversion();
        int children = node.getChildren().size();
        if (args.length == 3) {
            System.out.printf("%s cversion=%d children=%d zxid=0x%x%n", args[2], before, children, zxid);
            snapLog.close();
            return;
        }
        int target = Integer.parseInt(args[3]);
        synchronized (node) {
            node.stat.setCversion(target);
        }
        snapLog.save(tree, sessions, true);
        snapLog.close();
        System.out.printf("%s cversion %d -> %d, children=%d, 新快照 zxid=0x%x%n", args[2], before, target, children, zxid);
    }
}
