package iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.*;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class DataType {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        HadoopTables tables = new HadoopTables(conf);
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "event_time", Types.LongType.get()),
                Types.NestedField.required(3, "message", Types.StringType.get()),
                Types.NestedField.required(4, "t_timestamp", Types.TimestampType.withZone()),
                Types.NestedField.required(5, "t_date", Types.DateType.get()),
                Types.NestedField.required(6, "t_time", Types.TimeType.get()),
                Types.NestedField.required(7, "t_bool", Types.BooleanType.get()),
                Types.NestedField.required(8, "t_double", Types.DoubleType.get()),
                Types.NestedField.required(9, "t_float", Types.FloatType.get()),
                Types.NestedField.required(10, "t_decimal", Types.DecimalType.of(10, 2))
        );

        // 不指定namespace和表名，直接指定路径
        final String tableLocation = "file:///Users/huzekang/study/bigdata-iceberg/iceberg_warehouse/tb1";
        final ImmutableMap<String, String> pros = ImmutableMap.of(
                TableProperties.FORMAT_VERSION, "2",
                TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true",
                TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "1",
                TableProperties.MAX_SNAPSHOT_AGE_MS, 1000 * 60 * 2 + "",
                TableProperties.MANIFEST_MIN_MERGE_COUNT, "1"
        );
        tables.dropTable(tableLocation);
        tables.create(schema, null, null, pros, tableLocation);

        Table table = tables.load(tableLocation);

        // 写数据
        WriteResult result = appendWrite(schema, table);
        // 提交事务
        final AppendFiles appendFiles = table.newAppend();
        Arrays.stream(result.dataFiles()).forEach(dataFile -> appendFiles.appendFile(dataFile));
        appendFiles.commit();

        // 读取写入后数据
        System.out.println("写入数据后结果。。。。。");
        scanTable(table);



    }

    private static WriteResult appendWrite(Schema schema, Table table) throws IOException {
        final FileFormat fileFormat = FileFormat.valueOf("PARQUET");
        FileAppenderFactory<Record> appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(), null,
                table.schema(), null);

        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();

        // 非分区表可以直接用 UnpartitionedWriter，分区表可以用PartitionedWriter
        final UnpartitionedWriter unpartitionedWriter = new UnpartitionedWriter(null, fileFormat, appenderFactory,
                fileFactory, table.io(), 128 * 1024 * 1024);


        final GenericRecord gRecord = GenericRecord.create(schema);

        List<Record> expected = Lists.newArrayList();
        for (int i = 0; i < 5; i++) {
            HashMap<String, Object> map = new HashMap<>();
            map.put("id", i + 10);
            map.put("event_time", 1000L + i);
            map.put("message", String.format("新值-%d", i));
            map.put("t_timestamp", OffsetDateTime.now());
            map.put("t_date", LocalDate.now());
            map.put("t_time", LocalTime.now());
            map.put("t_bool", true);
            map.put("t_double", Double.valueOf(1.1D));
            map.put("t_float", 1.0f);
            map.put("t_decimal", BigDecimal.valueOf(1000,2));

            final Record record = gRecord.copy(map);


            expected.add(record);

            unpartitionedWriter.write(record);
        }
        WriteResult result = unpartitionedWriter.complete();
        return result;
    }


    private static void scanTable(Table table) {
        CloseableIterable<Record> scanResult = IcebergGenerics.read(table)
                .build();
        for (Record record : scanResult) {
            System.out.println(record.toString());
        }
    }
}
