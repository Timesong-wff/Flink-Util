package org.example;

import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

/**
 * @author wang ff
 * @date 2023/10/13 20:54
 *
 * 从Redis中根据key获取value，并返回
 */
public class DimUtil {
    //集群
    //取RowKey的一行数据,封装为jsonObj
    public static String getDimInfo(JedisCluster jedis, String tableName, String id) {
        //拼接从Redis中查询的key
        String redisKey = tableName.toLowerCase() + ":" + id;
        //接受Redis中查询的数据
        String dimStr = null;
        //接受方法的返回值
        String dimJsonObj = null;

        try {
            //先从缓存中获取维度数据
            dimStr = jedis.get(redisKey);

            if (StringUtils.isNotEmpty(dimStr)) {
                //System.out.println("=====>从Redis中查询到了数据<=====");
                //如果在缓存中找到了要查询的维度 , 直接返回 ----> 缓存命中
                dimJsonObj = dimStr;

            }
        } catch (Exception e) {
            //抛出异常，打断任务执行
            throw new RuntimeException("从Redis中查询维度数据发生异常");
            //e.printStackTrace();
            //System.out.println("=====>从Redis中查询维度数据发生了异常<=====");
        }
        return dimJsonObj;
    }

    //单机
    public static String getDimInfo(Jedis jedis, String tableName, String id) {
        //拼接从Redis中查询的key
        String redisKey = tableName.toLowerCase() + ":" + id;
        //接受Redis中查询的数据
        String dimStr = null;
        //接受方法的返回值
        String dimJsonObj = null;

        try {
            //先从缓存中获取维度数据
            dimStr = jedis.get(redisKey);

            if (StringUtils.isNotEmpty(dimStr)) {
                //System.out.println("=====>从Redis中查询到了数据<=====");
                //如果在缓存中找到了要查询的维度 , 直接返回 ----> 缓存命中
                dimJsonObj = dimStr;

            }
        } catch (Exception e) {
            //抛出异常，打断任务执行
            throw new RuntimeException("从Redis中查询维度数据发生异常");
            //e.printStackTrace();
            //System.out.println("=====>从Redis中查询维度数据发生了异常<=====");
        }
        return dimJsonObj;
    }

}
