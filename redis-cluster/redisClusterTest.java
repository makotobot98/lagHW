import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

public class redisClusterTest {
  public static void main(String[] args) {

    Set<HostAndPort> clusterNodes = new HashSet<>();
    clusterNodes.add(new HostAndPort("127.0.0.1", 7001));
    clusterNodes.add(new HostAndPort("127.0.0.1", 7002));
    clusterNodes.add(new HostAndPort("127.0.0.1", 7003));
    clusterNodes.add(new HostAndPort("127.0.0.1", 7004));
    clusterNodes.add(new HostAndPort("127.0.0.1", 7005));
    clusterNodes.add(new HostAndPort("127.0.0.1", 7006));
    clusterNodes.add(new HostAndPort("127.0.0.1", 7007));
    clusterNodes.add(new HostAndPort("127.0.0.1", 7008));

    JedisCluster cluster = new JedisCluster(clusterNodes);
    
    cluster.set("testKey", "testvalue");
    System.out.println(cluster.get("testKey"));

    cluster.close();
  }
}