package org.example;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

/**
 * @author WangFF
 * @version 1.0
 * @data 2024/3/11 9:53
 * 文件分桶策略
 */
public class HivePartitionBucketAssigner implements BucketAssigner<String, String> {
    @Override
    public String getBucketId(String s, Context context) {
        long l = System.currentTimeMillis();
        String date = DateFormatUtil.toYmdHms(l);
        return "dt="+date;
    }

    //初始化
    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
