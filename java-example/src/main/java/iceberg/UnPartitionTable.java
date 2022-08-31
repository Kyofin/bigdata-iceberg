package iceberg;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.*;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class UnPartitionTable {
    public static void main(String[] args) throws IOException, InterruptedException {
        createUnPartitionTable();
        write2UnPartitionTable();
        write2UnPartitionTableAppend();
        replace2UnPartitionTable();
        overwrite2UnPartitionTable();
        transactionAppend2UnPartitionTable();
    }

    public static void createUnPartitionTable() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.StringType.get()),
                Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
                Types.NestedField.required(3, "message", Types.StringType.get())
        );

        Configuration conf = new Configuration();
        HadoopTables tables = new HadoopTables(conf);
        // 不指定namespace和表名，直接指定路径
        final String tableLocation = "file:///Users/huzekang/study/bigdata-iceberg/iceberg_warehouse/tb2";
        tables.dropTable(tableLocation);
        Table table = tables.create(schema, null, tableLocation);

    }

    public static void write2UnPartitionTable() throws IOException {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "event_time", Types.LongType.get()),
                Types.NestedField.required(3, "message", Types.StringType.get())
        );
        Configuration conf = new Configuration();
        HadoopTables tables = new HadoopTables(conf);
        // 不指定namespace和表名，直接指定路径
        final String tableLocation = "file:///Users/huzekang/study/bigdata-iceberg/iceberg_warehouse/tb2";
        tables.dropTable(tableLocation);
        Table table = tables.create(schema, null, tableLocation);

        // 写数据
        final FileFormat fileFormat = FileFormat.valueOf("PARQUET");
        FileAppenderFactory<Record> appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(), null,
                table.schema(), null);

        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();

        // 非分区表可以直接用 UnpartitionedWriter，分区表可以用PartitionedWriter
        final MyTaskWriter taskWriter = new MyTaskWriter(table.spec(),
                fileFormat,
                appenderFactory,
                fileFactory,
                table.io(), 128 * 1024 * 1024);

        final GenericRecord gRecord = GenericRecord.create(schema);

        List<Record> expected = Lists.newArrayList();
        for (int i = 0; i < 5; i++) {
            final Record record = gRecord
                    .copy("id", i + 1, "event_time", System.currentTimeMillis(), "message", String.format("val-%d", i));
            expected.add(record);

            taskWriter.write(record);
        }
        WriteResult result = taskWriter.complete();
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

    public static void write2UnPartitionTableAppend() throws IOException {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "event_time", Types.LongType.get()),
                Types.NestedField.required(3, "message", Types.StringType.get())
        );
        Configuration conf = new Configuration();
        HadoopTables tables = new HadoopTables(conf);
        // 不指定namespace和表名，直接指定路径
        final String tableLocation = "file:///Users/huzekang/study/bigdata-iceberg/iceberg_warehouse/tb2";

        Table table = tables.load(tableLocation);

        // 写数据
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
                            "event_time", System.currentTimeMillis(),
                            "message", String.format("新值-%d", i));
            expected.add(record);

            unpartitionedWriter.write(record);
        }
        WriteResult result = unpartitionedWriter.complete();
        System.out.println("新增文件数：" + result.dataFiles().length);
        System.out.println("删除文件数：" + result.deleteFiles().length);
        // 提交事务
        final AppendFiles appendFiles = table.newAppend();
        Arrays.stream(result.dataFiles()).forEach(dataFile -> appendFiles.appendFile(dataFile));

        appendFiles.commit();
    }

    public static void replace2UnPartitionTable() throws IOException {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "event_time", Types.LongType.get()),
                Types.NestedField.required(3, "message", Types.StringType.get())
        );
        Configuration conf = new Configuration();
        HadoopTables tables = new HadoopTables(conf);
        // 不指定namespace和表名，直接指定路径
        final String tableLocation = "file:///Users/huzekang/study/bigdata-iceberg/iceberg_warehouse/tb2";
        Table table = tables.load( tableLocation);

        // 写数据
        final FileFormat fileFormat = FileFormat.valueOf("PARQUET");
        FileAppenderFactory<Record> appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(), null,
                table.schema(), null);

        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();

        // 非分区表可以直接用 UnpartitionedWriter，分区表可以用PartitionedWriter
        final MyTaskWriter taskWriter = new MyTaskWriter(table.spec(),
                fileFormat,
                appenderFactory,
                fileFactory,
                table.io(), 128 * 1024 * 1024);

        final GenericRecord gRecord = GenericRecord.create(schema);

        List<Record> expected = Lists.newArrayList();
        for (int i = 0; i < 5; i++) {
            final Record record = gRecord
                    .copy("id", i + 1, "event_time", System.currentTimeMillis(), "message", String.format("val-%d", i));
            expected.add(record);

            taskWriter.write(record);
        }
        WriteResult result = taskWriter.complete();
        System.out.println("新增文件数：" + result.dataFiles().length);
        System.out.println("删除文件数：" + result.deleteFiles().length);
        // 提交事务
        final ReplacePartitions replacePartitions = table.newReplacePartitions();
        Arrays.stream(result.dataFiles()).forEach(dataFile -> replacePartitions.addFile(dataFile));
        replacePartitions.commit();

    }

    public static void overwrite2UnPartitionTable() throws IOException {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "event_time", Types.LongType.get()),
                Types.NestedField.required(3, "message", Types.StringType.get())
        );
        Configuration conf = new Configuration();
        HadoopTables tables = new HadoopTables(conf);
        // 不指定namespace和表名，直接指定路径
        final String tableLocation = "file:///Users/huzekang/study/bigdata-iceberg/iceberg_warehouse/tb2";
        Table table = tables.load( tableLocation);

        // 写数据
        final FileFormat fileFormat = FileFormat.valueOf("PARQUET");
        FileAppenderFactory<Record> appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(), null,
                table.schema(), null);

        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();

        // 非分区表可以直接用 UnpartitionedWriter，分区表可以用PartitionedWriter
        final MyTaskWriter taskWriter = new MyTaskWriter(table.spec(),
                fileFormat,
                appenderFactory,
                fileFactory,
                table.io(), 128 * 1024 * 1024);

        final GenericRecord gRecord = GenericRecord.create(schema);

        List<Record> expected = Lists.newArrayList();
        for (int i = 0; i < 5; i++) {
            final Record record = gRecord
                    .copy("id", i + 1, "event_time", System.currentTimeMillis(), "message", String.format("val-%d", i));
            expected.add(record);

            taskWriter.write(record);
        }
        WriteResult result = taskWriter.complete();
        System.out.println("新增文件数：" + result.dataFiles().length);
        System.out.println("删除文件数：" + result.deleteFiles().length);
        // 提交事务
        final OverwriteFiles overwriteFiles = table.newOverwrite();
        overwriteFiles.overwriteByRowFilter(Expressions.alwaysTrue());
        Arrays.stream(result.dataFiles()).forEach(dataFile -> overwriteFiles.addFile(dataFile));
        overwriteFiles.commit();

    }

    public static void transactionAppend2UnPartitionTable() throws IOException, InterruptedException {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "event_time", Types.LongType.get()),
                Types.NestedField.required(3, "message", Types.StringType.get())
        );
        Configuration conf = new Configuration();
        HadoopTables tables = new HadoopTables(conf);
        // 不指定namespace和表名，直接指定路径
        final String tableLocation = "file:///Users/huzekang/study/bigdata-iceberg/iceberg_warehouse/tb2";
        Table table = tables.load(tableLocation);

        // 写数据
        final FileFormat fileFormat = FileFormat.valueOf("PARQUET");
        FileAppenderFactory<Record> appenderFactory = new GenericAppenderFactory(table.schema(), table.spec(), null,
                table.schema(), null);

        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();

        List<DataFile> dataFileList = Collections.synchronizedList(new ArrayList<>());

        class Task implements Runnable{
            @Override
            public void run() {
                // 非分区表可以直接用 UnpartitionedWriter，分区表可以用PartitionedWriter
                final MyTaskWriter taskWriter = new MyTaskWriter(table.spec(),
                        fileFormat,
                        appenderFactory,
                        fileFactory,
                        table.io(), 128 * 1024 * 1024);
                final GenericRecord gRecord = GenericRecord.create(schema);
                List<Record> expected = Lists.newArrayList();
                for (int i = 0; i < 5; i++) {
                    final Record record = gRecord
                            .copy("id", i + 1, "event_time", System.currentTimeMillis(), "message",
                                    String.format(Thread.currentThread().getId() + "-val-%d", i));
                    expected.add(record);

                    try {
                        taskWriter.write(record);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                WriteResult result = null;
                try {
                    result = taskWriter.complete();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                Arrays.stream(result.dataFiles()).forEach(dataFile -> dataFileList.add(dataFile));
            }
        }
        //样例2 Common Thread Pool
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("consumer-queue-thread-%d").build();
        ExecutorService pool2 = new ThreadPoolExecutor(5, 8,0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(1024), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());

        //提交任务到线程池
        for (int I = 0; I < 20; I++) {
            pool2.execute(new Task());
        }
        //优雅关闭
        pool2.shutdown();
        while (!pool2.awaitTermination(1, TimeUnit.SECONDS)) {
            System.out.println("线程还在执行。。。");
        }
        System.out.println("线程都ok！");
        final Transaction transaction = table.newTransaction();
        final AppendFiles appendFiles = transaction.newAppend();
        dataFileList.forEach(dataFile -> appendFiles.appendFile(dataFile));
        appendFiles.commit();
        transaction.commitTransaction();

    }

    private static class MyTaskWriter extends BaseTaskWriter<Record> {

        private RollingFileWriter currentWriter;

        private MyTaskWriter(PartitionSpec spec, FileFormat format,
                             FileAppenderFactory<Record> appenderFactory,
                             OutputFileFactory fileFactory, FileIO io,
                             long targetFileSize) {
            super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
            this.currentWriter = new RollingFileWriter(null);

        }

        @Override
        public void write(Record row) throws IOException {
            currentWriter.write(row);
        }


        @Override
        public void close() throws IOException {
            if (currentWriter != null) {
                currentWriter.close();
            }

        }
    }

}
