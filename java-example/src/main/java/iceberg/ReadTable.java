package iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;

public class ReadTable {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        HadoopTables tables = new HadoopTables(conf);
        // 不指定namespace和表名，直接指定路径
        final String tableLocation = "file:///Users/huzekang/study/bigdata-iceberg/iceberg_warehouse/tb3";
        final Table table = tables.load(tableLocation);

        table.history().forEach(historyEntry -> {
            System.out.println("快照id:"+historyEntry.snapshotId());
        });

        table.snapshots().forEach(snapshot -> {
            System.out.println("--------------快照概要：" + snapshot.snapshotId());
            snapshot.summary().forEach((k, v) -> {
                System.out.println(k + ":" + v);
            });
        });


        // 根据数据找文件
        TableScan scan = table.newScan();
        TableScan filteredScan = scan.filter(Expressions.equal("id", 10));
        Schema projection = scan.schema();
        Iterable<CombinedScanTask> tasks = filteredScan.planTasks();

        for (CombinedScanTask task : tasks) {
            for (FileScanTask file : task.files()) {
                System.out.println("数据所在文件："+file.file().toString());
            }
        }

        // 根据字段找每一行数据
        CloseableIterable<Record> result = IcebergGenerics.read(table)
                .where(Expressions.greaterThan("id", 10))
                .build();
        for (Record record : result) {
            System.out.println(record.toString());
        }

    }
}
