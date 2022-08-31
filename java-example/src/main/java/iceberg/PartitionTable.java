package iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class PartitionTable {
    public static void main(String[] args) throws IOException {
        write2PartitionTable();
        overwrite2PartitionTable();
        replace2PartitionTable();
    }
    public static void write2PartitionTable() throws IOException {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "province", Types.StringType.get()),
                Types.NestedField.required(3, "city", Types.StringType.get()),
                Types.NestedField.required(4, "message", Types.StringType.get())
        );
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("province")
                .identity("city")
                .build();

        Configuration conf = new Configuration();
        HadoopTables tables = new HadoopTables(conf);
        // 不指定namespace和表名，直接指定路径
        final String tableLocation = "file:///Users/huzekang/study/bigdata-iceberg/iceberg_warehouse/part_tb3";
        final ImmutableMap<String, String> pros = ImmutableMap.of(
                TableProperties.FORMAT_VERSION, "2"
        );
        tables.dropTable(tableLocation);
        Table table = tables.create(schema, spec, pros, tableLocation);

        // 写数据
        final FileFormat fileFormat = FileFormat.valueOf("PARQUET");
        FileAppenderFactory<Record> appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(), null,
                table.schema(), null);

        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();

        // 非分区表可以直接用 UnpartitionedWriter，分区表可以用PartitionedWriter
        final PartitionedWriter partitionedWriter = new PartitionedWriter(spec, fileFormat, appenderFactory,
                fileFactory, table.io(), 128 * 1024 * 1024) {
            @Override
            protected PartitionKey partition(Object row) {
                final GenericRecord genericRecord = (GenericRecord) row;
                final PartitionKey partitionKey = new PartitionKey(spec, schema);
                partitionKey.partition(genericRecord);
                return partitionKey;
            }
        };

        final GenericRecord gRecord = GenericRecord.create(schema);

        List<Record> expected = Lists.newArrayList();
        for (int i = 0; i < 5; i++) {
            final HashMap<String, Object> hashMap = Maps.newHashMap();
            hashMap.put("id", i + 1);
            hashMap.put("province", "gd");
            hashMap.put("city", "shenzhen");
            hashMap.put("message", String.format("msg-%d", i));
            final Record record = gRecord.copy(hashMap);
            expected.add(record);

            partitionedWriter.write(record);
        }
        WriteResult result = partitionedWriter.complete();
        System.out.println("新增文件数：" + result.dataFiles().length);
        System.out.println("删除文件数：" + result.deleteFiles().length);
        // 提交事务
        RowDelta rowDelta = table.newRowDelta();
        Arrays.stream(result.dataFiles()).forEach(dataFile -> rowDelta.addRows(dataFile));
        Arrays.stream(result.deleteFiles()).forEach(dataFile -> rowDelta.addDeletes(dataFile));

        rowDelta.validateDeletedFiles()
                .validateDataFilesExist(Lists.newArrayList(result.referencedDataFiles()))
                .commit();

    }

    public static void overwrite2PartitionTable() throws IOException {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "province", Types.StringType.get()),
                Types.NestedField.required(3, "city", Types.StringType.get()),
                Types.NestedField.required(4, "message", Types.StringType.get())
        );
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("province")
                .identity("city")
                .build();

        Configuration conf = new Configuration();
        HadoopTables tables = new HadoopTables(conf);
        // 不指定namespace和表名，直接指定路径
        final String tableLocation = "file:///Users/huzekang/study/bigdata-iceberg/iceberg_warehouse/part_tb3";

        Table table = tables.load( tableLocation);

        // 写数据
        final FileFormat fileFormat = FileFormat.valueOf("PARQUET");
        FileAppenderFactory<Record> appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(), null,
                table.schema(), null);

        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();

        // 非分区表可以直接用 UnpartitionedWriter，分区表可以用PartitionedWriter
        final PartitionedWriter partitionedWriter = new PartitionedWriter(spec, fileFormat, appenderFactory,
                fileFactory, table.io(), 128 * 1024 * 1024) {
            @Override
            protected PartitionKey partition(Object row) {
                final GenericRecord genericRecord = (GenericRecord) row;
                final PartitionKey partitionKey = new PartitionKey(spec, schema);
                partitionKey.partition(genericRecord);
                return partitionKey;
            }
        };

        final GenericRecord gRecord = GenericRecord.create(schema);

        List<Record> expected = Lists.newArrayList();
        for (int i = 0; i < 5; i++) {
            final HashMap<String, Object> hashMap = Maps.newHashMap();
            hashMap.put("id", i + 1);
            hashMap.put("province", "gd");
            hashMap.put("city", "shenzhen");
            hashMap.put("message", String.format("覆盖-msg-%d", i*20));
            final Record record = gRecord.copy(hashMap);
            expected.add(record);

            partitionedWriter.write(record);
        }
        WriteResult result = partitionedWriter.complete();
        System.out.println("新增文件数：" + result.dataFiles().length);
        System.out.println("删除文件数：" + result.deleteFiles().length);
        // 提交事务
        final OverwriteFiles overwriteFiles = table.newOverwrite();
        Arrays.stream(result.dataFiles()).forEach(dataFile -> overwriteFiles.addFile(dataFile));

        overwriteFiles.overwriteByRowFilter(Expressions.and(Expressions.equal("city","shenzhen"),Expressions.equal("province","gd")))
                .commit();


    }

    public static void replace2PartitionTable() throws IOException {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "province", Types.StringType.get()),
                Types.NestedField.required(3, "city", Types.StringType.get()),
                Types.NestedField.required(4, "message", Types.StringType.get())
        );
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("province")
                .identity("city")
                .build();

        Configuration conf = new Configuration();
        HadoopTables tables = new HadoopTables(conf);
        // 不指定namespace和表名，直接指定路径
        final String tableLocation = "file:///Users/huzekang/study/bigdata-iceberg/iceberg_warehouse/part_tb3";
        Table table = tables.load(tableLocation);

        // 写数据
        final FileFormat fileFormat = FileFormat.valueOf("PARQUET");
        FileAppenderFactory<Record> appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(), null,
                table.schema(), null);

        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();

        // 非分区表可以直接用 UnpartitionedWriter，分区表可以用PartitionedWriter
        final PartitionedWriter partitionedWriter = new PartitionedWriter(spec, fileFormat, appenderFactory,
                fileFactory, table.io(), 128 * 1024 * 1024) {
            @Override
            protected PartitionKey partition(Object row) {
                final GenericRecord genericRecord = (GenericRecord) row;
                final PartitionKey partitionKey = new PartitionKey(spec, schema);
                partitionKey.partition(genericRecord);
                return partitionKey;
            }
        };

        final GenericRecord gRecord = GenericRecord.create(schema);

        List<Record> expected = Lists.newArrayList();
        for (int i = 0; i < 5; i++) {
            final HashMap<String, Object> hashMap = Maps.newHashMap();
            hashMap.put("id", i + 1);
            hashMap.put("province", "gd");
            hashMap.put("city", "shenzhen");
            hashMap.put("message", String.format("msg-%d", i * 20));
            final Record record = gRecord.copy(hashMap);
            expected.add(record);

            partitionedWriter.write(record);
        }
        WriteResult result = partitionedWriter.complete();
        System.out.println("新增文件数：" + result.dataFiles().length);
        System.out.println("删除文件数：" + result.deleteFiles().length);

        //ReplacePartitions
        final ReplacePartitions replacePartitions = table.newReplacePartitions();
        Arrays.stream(result.dataFiles()).forEach(e->replacePartitions.addFile(e));
        replacePartitions.commit();

    }
}
