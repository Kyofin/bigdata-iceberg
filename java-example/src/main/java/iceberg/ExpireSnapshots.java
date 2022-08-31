package iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

public class ExpireSnapshots {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        HadoopTables tables = new HadoopTables(conf);
        // 不指定namespace和表名，直接指定路径
        final String tableLocation = "file:///Users/huzekang/study/bigdata-iceberg/iceberg_warehouse/tb5";
        final Table table = tables.load(tableLocation);
        // 删除过期的元数据快照: 会同时删除Manifests【xxx.avro】  和 Manifests Lists【snap-xxx.avro】
        long tsToExpire = System.currentTimeMillis() - (1000 * 60 * 1);
        table.expireSnapshots()
                .expireOlderThan(tsToExpire)
                .commit();
    }
}
