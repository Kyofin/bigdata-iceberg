package iceberg;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;

public class FormatV2Table {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        HadoopTables tables = new HadoopTables(conf);
        Schema schema = new Schema(
                Types.NestedField.required(1, "level", Types.StringType.get()),
                Types.NestedField.required(2, "event_time", Types.LongType.get()),
                Types.NestedField.required(3, "message", Types.StringType.get())
        );

        // 不指定namespace和表名，直接指定路径
        final String tableLocation = "file:///Users/huzekang/study/bigdata-iceberg/iceberg_warehouse/tb5";
        final ImmutableMap<String, String> pros = ImmutableMap.of(
                TableProperties.FORMAT_VERSION, "2",
                TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true",
                TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "1",
                TableProperties.MAX_SNAPSHOT_AGE_MS, 1000 * 60 * 2 + "",
                TableProperties.MANIFEST_MIN_MERGE_COUNT, "2"
        );
        tables.dropTable(tableLocation);
        tables.create(schema, null, null, pros, tableLocation);
    }
}
