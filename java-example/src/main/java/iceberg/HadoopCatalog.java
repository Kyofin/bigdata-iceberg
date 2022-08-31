package iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

public class HadoopCatalog {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        String warehousePath = "file:///Users/huzekang/study/bigdata-iceberg/iceberg_warehouse";
        org.apache.iceberg.hadoop.HadoopCatalog catalog = new org.apache.iceberg.hadoop.HadoopCatalog(conf, warehousePath);

        Schema schema = new Schema(
                Types.NestedField.required(1, "level", Types.StringType.get()),
                Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
                Types.NestedField.required(3, "message", Types.StringType.get()),
                Types.NestedField.optional(4, "call_stack", Types.ListType.ofRequired(5, Types.StringType.get()))
        );

        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .hour("event_time")
                .identity("level")
                .build();

        TableIdentifier name = TableIdentifier.of("logging.db", "logs");
        // 删除分区表
        catalog.dropTable(name);
        // 创建分区表
        Table table = catalog.createTable(name, schema, spec);

        // 加载表
        final Table loadTable = catalog.loadTable(name);
        System.out.println(loadTable.history().size());
    }
}
