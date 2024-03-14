package org.example;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @author WangFF
 * @version 1.0
 * @data 2024/3/11 10:00
 * 流处理写入HDFS
 */
public class FileSink {
    public static StreamingFileSink<String> getFileSink(){
        System.setProperty("HADOOP_USER_NAME","xxxx");
        System.setProperty("Password","xxxxx");
        //2. 创建StreamingFileSink对象
        StreamingFileSink<String> fileSink = StreamingFileSink
                // 4-1. 设置存储文件格式，Row一行一行数据存储
                .<String>forRowFormat(
                        new Path("hdfs://192.168.100.102:8020/user/hive/warehouse/vehicle_ods.db/"), new SimpleStringEncoder<String>("UTF-8")
                )
                // 4-2. 设置桶分配策略，存储目录名称，默认基于事件分配器
                .withBucketAssigner(
//                        new DateTimeBucketAssigner<String>("yyyyDDmm")
                        new HivePartitionBucketAssigner()
                )

                // 4-3. 设置数据文件滚动策略，如何产生新文件
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                // 时间间隔 2分钟写入一次,如果1分钟没有数据写入 就自动写入
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(10))
                                // 多久不写入数据时间间隔
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                                // 文件大小
                                .withMaxPartSize(128 * 1024 * 1024)
                                .build()
                )
                // 4-4. 设置文件名称  车辆数据
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("xxxx")
                                .withPartSuffix(".data")
                                .build()
                )
                .build();

        return fileSink;
    }

}
