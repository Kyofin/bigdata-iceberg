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
import java.util.Arrays;
import java.util.List;

public class DeleteTable {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        HadoopTables tables = new HadoopTables(conf);
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "event_time", Types.LongType.get()),
                Types.NestedField.optional(3, "message", Types.StringType.get())
        );

        // 不指定namespace和表名，直接指定路径
        final String tableLocation = "file:///Users/huzekang/study/bigdata-iceberg/iceberg_warehouse/tb4";
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

        // 删除数据
        List<DeleteFile> deleteFiles1 = deleteWrite(schema, table);
        RowDelta rowDelta = table.newRowDelta();
        for (DeleteFile deleteFile : deleteFiles1) {
            rowDelta.addDeletes(deleteFile);
        }
        rowDelta.commit();

        // 读取写入后数据
        System.out.println("删除数据后结果。。。。。");
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
            final Record record = gRecord
                    .copy("id", i + 10,
                            "event_time", 1000L + i,
                            "message", String.format("新值-%d", i));
            expected.add(record);

            unpartitionedWriter.write(record);
        }
        WriteResult result = unpartitionedWriter.complete();
        return result;
    }

    private static List<DeleteFile> deleteWrite(Schema schema, Table table) throws IOException {
        final FileFormat fileFormat = FileFormat.valueOf("PARQUET");
        int[] equalityFieldIds = {schema.findField("id").fieldId()};
        FileAppenderFactory<Record> appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(), equalityFieldIds,
                table.schema(), null);

        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();

        EncryptedOutputFile outputFile = fileFactory.newOutputFile();
        EqualityDeleteWriter<Record> recordEqualityDeleteWriter = appenderFactory.newEqDeleteWriter(outputFile, fileFormat, null);
        final GenericRecord gRecord = GenericRecord.create(schema);

        recordEqualityDeleteWriter.write( gRecord.copy("id", 10));
        recordEqualityDeleteWriter.write( gRecord.copy("id", 11));
        recordEqualityDeleteWriter.write( gRecord.copy("id", 12));

        recordEqualityDeleteWriter.close();
        DeleteWriteResult deleteWriteResult = recordEqualityDeleteWriter.result();
        List<DeleteFile> deleteFiles = deleteWriteResult.deleteFiles();

        return deleteFiles;
    }

    private static void scanTable(Table table) {
        CloseableIterable<Record> scanResult = IcebergGenerics.read(table)
                .build();
         for (Record record : scanResult) {
            System.out.println(record.toString());
        }
    }
}
