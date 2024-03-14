package org.example;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;

import java.util.HashSet;
import java.util.Set;

/**
 *  通过单列加锁的方式进行连接
 *  使用JedisPool来获取连接，确保线程安全
 */

public class RedisUtil {
    //获取连接通过单例实现 , 获取一次连接就够了
    private static JedisPool jedisPool;

    public static Jedis getJedis() {
        //单例 ，加锁，先判断jedis是否为空，为空则创建连接，两次判断
        if (jedisPool == null) {
            synchronized (RedisUtil.class) {
                if (jedisPool == null) {
                    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
                    jedisPoolConfig.setMaxTotal(400);//最大连接数
                    jedisPoolConfig.setMinIdle(5);//最小连接数
                    jedisPoolConfig.setMaxIdle(5);//最大空闲达到,然后进行缩容
                    jedisPoolConfig.setBlockWhenExhausted(true);//是否等待
                    jedisPoolConfig.setMaxWaitMillis(2000);//等待时间
                    jedisPoolConfig.setTestOnBorrow(true);//连接时进行测试ping pong
                    jedisPool = new JedisPool(jedisPoolConfig, "hadoop102", 6379, 10000);
                }
            }
        }
        System.out.println("getJedis");
        return jedisPool.getResource();
    }

    public static void closeJedis(Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }

    private static JedisCluster jedisCluster;

    //获取Redis连接
    public static JedisCluster getRedisClient() {
        if (jedisPool == null) {
            synchronized (RedisUtil.class) {
                if (jedisPool == null) {
                    GenericObjectPoolConfig config = new GenericObjectPoolConfig();
                    config.setMaxIdle(50); //设置连接池的最大空闲连接数
                    config.setMaxTotal(300); //设置连接池的最大连接数
                    config.setMaxWaitMillis(2000); //设置连接池的最大等待时间
                    config.setBlockWhenExhausted(true); //设置当连接池满了之后，是否阻塞，true阻塞，false不阻塞
                    config.setTestOnBorrow(true); //在从连接池中借出连接时对连接进行可用性测试
                    Set<HostAndPort> jedisClusterNode = new HashSet<HostAndPort>();
                    jedisClusterNode.add(new HostAndPort("134.80.157.67", 16380));
                    jedisClusterNode.add(new HostAndPort("134.80.157.67", 16381));
                    jedisClusterNode.add(new HostAndPort("134.80.157.67", 16382));
                    jedisClusterNode.add(new HostAndPort("134.80.157.67", 16383));
                    jedisClusterNode.add(new HostAndPort("134.80.157.67", 16384));
                    jedisClusterNode.add(new HostAndPort("134.80.157.67", 16385));
                    jedisClusterNode.add(new HostAndPort("134.80.157.67", 16386));
                    jedisClusterNode.add(new HostAndPort("134.80.157.67", 16387));

                    jedisClusterNode.add(new HostAndPort("134.80.157.68", 16381));
                    jedisClusterNode.add(new HostAndPort("134.80.157.68", 16382));
                    jedisClusterNode.add(new HostAndPort("134.80.157.68", 16383));
                    jedisClusterNode.add(new HostAndPort("134.80.157.68", 16384));
                    jedisClusterNode.add(new HostAndPort("134.80.157.68", 16385));
                    jedisClusterNode.add(new HostAndPort("134.80.157.68", 16386));
                    jedisClusterNode.add(new HostAndPort("134.80.157.68", 16387));
                    jedisClusterNode.add(new HostAndPort("134.80.157.68", 16380));

                    jedisClusterNode.add(new HostAndPort("134.80.157.69", 16380));
                    jedisClusterNode.add(new HostAndPort("134.80.157.69", 16381));
                    jedisClusterNode.add(new HostAndPort("134.80.157.69", 16382));
                    jedisClusterNode.add(new HostAndPort("134.80.157.69", 16383));
                    jedisClusterNode.add(new HostAndPort("134.80.157.69", 16384));
                    jedisClusterNode.add(new HostAndPort("134.80.157.69", 16385));
                    jedisClusterNode.add(new HostAndPort("134.80.157.69", 16386));
                    jedisClusterNode.add(new HostAndPort("134.80.157.69", 16387));

                    jedisClusterNode.add(new HostAndPort("134.80.157.70", 16380));
                    jedisClusterNode.add(new HostAndPort("134.80.157.70", 16381));
                    jedisClusterNode.add(new HostAndPort("134.80.157.70", 16382));
                    jedisClusterNode.add(new HostAndPort("134.80.157.70", 16383));
                    jedisClusterNode.add(new HostAndPort("134.80.157.70", 16384));
                    jedisClusterNode.add(new HostAndPort("134.80.157.70", 16385));
                    jedisClusterNode.add(new HostAndPort("134.80.157.70", 16386));
                    jedisClusterNode.add(new HostAndPort("134.80.157.70", 16387));

                    jedisClusterNode.add(new HostAndPort("134.80.157.71", 16380));
                    jedisClusterNode.add(new HostAndPort("134.80.157.71", 16381));
                    jedisClusterNode.add(new HostAndPort("134.80.157.71", 16382));
                    jedisClusterNode.add(new HostAndPort("134.80.157.71", 16383));
                    jedisClusterNode.add(new HostAndPort("134.80.157.71", 16384));
                    jedisClusterNode.add(new HostAndPort("134.80.157.71", 16385));
                    jedisClusterNode.add(new HostAndPort("134.80.157.71", 16386));
                    jedisClusterNode.add(new HostAndPort("134.80.157.71", 16387));

                    jedisClusterNode.add(new HostAndPort("134.80.157.72", 16380));
                    jedisClusterNode.add(new HostAndPort("134.80.157.72", 16381));
                    jedisClusterNode.add(new HostAndPort("134.80.157.72", 16382));
                    jedisClusterNode.add(new HostAndPort("134.80.157.72", 16383));
                    jedisClusterNode.add(new HostAndPort("134.80.157.72", 16384));
                    jedisClusterNode.add(new HostAndPort("134.80.157.72", 16385));
                    jedisClusterNode.add(new HostAndPort("134.80.157.72", 16386));
                    jedisClusterNode.add(new HostAndPort("134.80.157.72", 16387));

                    jedisClusterNode.add(new HostAndPort("134.80.157.73", 16380));
                    jedisClusterNode.add(new HostAndPort("134.80.157.73", 16381));
                    jedisClusterNode.add(new HostAndPort("134.80.157.73", 16382));
                    jedisClusterNode.add(new HostAndPort("134.80.157.73", 16383));
                    jedisClusterNode.add(new HostAndPort("134.80.157.73", 16384));
                    jedisClusterNode.add(new HostAndPort("134.80.157.73", 16385));
                    jedisClusterNode.add(new HostAndPort("134.80.157.73", 16386));
                    jedisClusterNode.add(new HostAndPort("134.80.157.73", 16387));

                    jedisClusterNode.add(new HostAndPort("134.80.157.74", 16380));
                    jedisClusterNode.add(new HostAndPort("134.80.157.74", 16381));
                    jedisClusterNode.add(new HostAndPort("134.80.157.74", 16382));
                    jedisClusterNode.add(new HostAndPort("134.80.157.74", 16383));
                    jedisClusterNode.add(new HostAndPort("134.80.157.74", 16384));
                    jedisClusterNode.add(new HostAndPort("134.80.157.74", 16385));
                    jedisClusterNode.add(new HostAndPort("134.80.157.74", 16386));
                    jedisClusterNode.add(new HostAndPort("134.80.157.74", 16387));


                    jedisCluster = new JedisCluster(jedisClusterNode, 2000, 2000, 5, "Redis@6.0.6", config);
                }
            }
        }
        return jedisCluster;
    }

    //关闭Redis客户端Jedis
    public static void closeRedisClient(JedisCluster jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }

    /*public static void main(String[] args) {
        Jedis jedis = getJedis();
        String s = jedis.get("dim_redis_label_res_app:123456");
        System.out.println(s);
        closeJedis(jedis);
    }*/


}
