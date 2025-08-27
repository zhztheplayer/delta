/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.parquet.velox;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.arrow.Arrow;
import io.github.zhztheplayer.velox4j.config.Config;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
import io.github.zhztheplayer.velox4j.connector.*;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.expression.CastTypedExpr;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.iterator.CloseableIterator;
import io.github.zhztheplayer.velox4j.iterator.UpIterators;
import io.github.zhztheplayer.velox4j.memory.BytesAllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.ProjectNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.query.SerialTask;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.type.RowType;
import io.github.zhztheplayer.velox4j.type.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.parquet.ParquetColumn;
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport$;
import org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import scala.Option;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Stream;

public class VeloxVectorizedParquetRecordReader extends RecordReader<Void, Object> {
  static {
    VeloxInitializer.ensureInitialized();
  }

  private final int capacity;
  private final ZoneId convertTz;
  private final String datetimeRebaseMode;
  private final String datetimeRebaseTz;
  private final String int96RebaseMode;
  private final String int96RebaseTz;
  private final boolean isCaseSensitive;


  private boolean returnColumnarBatch = false;

  private boolean initialized = false;
  private Path file = null;
  private MessageType fileParquetSchema = null;
  private MessageType clippedParquetSchema = null;
  private StructType sparkFileClippedSchema = null;
  private StructType sparkSchema = null;
  private ParquetColumn parquetColumn = null;

  private Set<ParquetColumn> missingColumns = null;
  private BytesAllocationListener listener = null;
  private BufferAllocator bufferAllocator = null;
  private MemoryManager memoryManager = null;
  private Session session = null;
  private SerialTask task = null;
  private CloseableIterator<RowVector> veloxOutIterator = null;

  private StructType partitionColumns = null;
  private InternalRow partitionValues = null;
  private StructVector arrowDataStructVector = null;
  private ConstantColumnVector[] partitionVectors = null;
  private ColumnarBatch columnarBatch = null;
  private int rowOffsetInBatch = 0;

  private long rowsReturned = 0;
  private long totalRowCount = 0;
  private List<Filter> filters = null;

  public VeloxVectorizedParquetRecordReader(
      ZoneId convertTz,
      String datetimeRebaseMode,
      String datetimeRebaseTz,
      String int96RebaseMode,
      String int96RebaseTz,
      boolean useOffHeap,
      boolean isCaseSensitive,
      int capacity) {
    Preconditions.checkArgument(useOffHeap, "Velox-based reader doesn't support on-heap mode");
    this.convertTz = convertTz;
    this.datetimeRebaseMode = datetimeRebaseMode;
    this.datetimeRebaseTz = datetimeRebaseTz;
    this.int96RebaseMode = int96RebaseMode;
    this.int96RebaseTz = int96RebaseTz;
    this.isCaseSensitive = isCaseSensitive;
    this.capacity = capacity;
  }

  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext, Option<ParquetMetadata> fileFooter, List<Filter> filters) throws IOException, InterruptedException {
    this.filters = filters; // TODO: Filter push down to Velox.
    final Configuration configuration = taskAttemptContext.getConfiguration();
    final FileSplit split = (FileSplit) inputSplit;
    this.file = split.getPath();


    final Map<String, String> fileMetadata;
    // A reader to infer schema.
    final ParquetFileReader fileReader;
      if (fileFooter.isDefined()) {
        fileReader = new ParquetFileReader(configuration, file, fileFooter.get());
      } else {
        ParquetReadOptions options = HadoopReadOptions
            .builder(configuration, file)
            .withRange(split.getStart(), split.getStart() + split.getLength())
            .build();
        fileReader = new ParquetFileReader(
            HadoopInputFile.fromPath(file, configuration), options);
      }
    try {
      fileParquetSchema = fileReader.getFileMetaData().getSchema();
      fileMetadata = fileReader.getFileMetaData().getKeyValueMetaData();
    } finally {
      fileReader.close();
    }


    ReadSupport<Object> readSupport = getReadSupportInstance(getReadSupportClass(configuration));
    ReadSupport.ReadContext readContext = readSupport.init(new InitContext(
        taskAttemptContext.getConfiguration(), toSetMultiMap(fileMetadata), fileParquetSchema));
    this.clippedParquetSchema = readContext.getRequestedSchema();
    final ParquetToSparkSchemaConverter converter = new ParquetToSparkSchemaConverter(configuration);
    final String sparkRequestedSchemaString =
        configuration.get(ParquetReadSupport$.MODULE$.SPARK_ROW_REQUESTED_SCHEMA());
    final StructType sparkRequestedSchema = StructType$.MODULE$.fromString(sparkRequestedSchemaString);
    this.parquetColumn = converter.convertParquetColumn(clippedParquetSchema, Option.apply(sparkRequestedSchema));
    this.sparkSchema = (StructType) parquetColumn.sparkType();
    this.sparkFileClippedSchema = converter.convert(clippedParquetSchema);

    this.missingColumns = new HashSet<>();
    this.listener = new BytesAllocationListener();
    this.bufferAllocator = new RootAllocator();
    this.memoryManager = Velox4j.newMemoryManager(listener);
    this.session = Velox4j.newSession(memoryManager);
    final String timeZoneId = convertTz == null ? ZoneOffset.UTC.getId() : convertTz.getId();
    final VeloxConverter veloxConverter = VeloxConverter.of(session, bufferAllocator, timeZoneId, isCaseSensitive);


    final PlanNode planNode;

    // 1. Creates the scan node.
    final RowType scanOutputType = veloxConverter.toVeloxRowType(
        isCaseSensitive ? sparkFileClippedSchema : (StructType) toLowerCase(sparkFileClippedSchema));
    final TypedExpr remainingFilter = this.filters.stream()
        .reduce(And::new)
        .map(mergedSparkFilter ->
            veloxConverter.filterToVeloxExpr(scanOutputType, mergedSparkFilter)
        )
        .orElse(null); // Null in Velox means no filters.
    final TableScanNode scanNode = new TableScanNode(
        "scan-node",
        scanOutputType,
        new HiveTableHandle(
            "connector-hive",
            "table-1",
            true,
            Collections.emptyList(),
            remainingFilter,
            scanOutputType,
            Collections.emptyMap()
        ),
        toAssignments(scanOutputType)
    );

    // 2. Creates the project node that casts scan node's output to align with Spark's required output types.
    final RowType outputType = veloxConverter.toVeloxRowType(isCaseSensitive ? sparkSchema: (StructType) toLowerCase(sparkSchema));
    Preconditions.checkState(outputType.size() == scanOutputType.size());

    if (needsCasting(scanOutputType, outputType)) {
      final List<TypedExpr> projections = createCasts(scanOutputType, outputType);
      planNode = new ProjectNode("proj-node:cast-scan-schema",
          Collections.singletonList(scanNode), outputType.getNames(), projections);
    } else {
      planNode = scanNode;
    }

    // 3. Build the query.
    final Query query = new Query(planNode,
        Config.create(
            ImmutableMap.<String, String>builder()
                .put("preferred_output_batch_rows", Integer.toString(capacity))
                .put("max_output_batch_rows", Integer.toString(capacity))
                .build()),
        ConnectorConfig.create(
            ImmutableMap.<String, Config>builder()
                .put("connector-hive",
                    Config.create(
                        ImmutableMap.<String, String>builder()
                            // Reads all columns into lower cases for case insensitivity.
                            .put("file_column_names_read_as_lower_case", Boolean.toString(!isCaseSensitive))
                            .build()
                    )
                )
                .build()
        ));
    task = session.queryOps().execute(query);
    final FileSplit fileSplit = (FileSplit) inputSplit;
    final ConnectorSplit connectorSplit = new HiveConnectorSplit(
        "connector-hive",
        0,
        false,
        fileSplit.getPath().toString(),
        FileFormat.PARQUET,
        fileSplit.getStart(),
        fileSplit.getLength(),
        Collections.emptyMap(),
        null,
        null,
        Collections.emptyMap(),
        null,
        Collections.emptyMap(),
        Collections.emptyMap(),
        null,
        null
    );
    task.addSplit(scanNode.getId(), connectorSplit);
    task.noMoreSplits(scanNode.getId());
    veloxOutIterator = UpIterators.asJavaIterator(task);
    initialized = true;
  }

  private boolean needsCasting(RowType fromRowType, RowType toRowType) {
    return true; // FIXME: cast only as needed.
  }

  private List<TypedExpr> createCasts(RowType fromRowType, RowType toRowType) {
    final List<TypedExpr> casts = new ArrayList<>(toRowType.size());
    for (int i = 0; i < toRowType.size(); i++) {
      final Type fromType = fromRowType.getChildren().get(i);
      final Type toType = toRowType.getChildren().get(i);
      casts.add(
          CastTypedExpr.create(toType, FieldAccessTypedExpr.create(fromType, fromRowType.getNames().get(i)), false));
    }
    return Collections.unmodifiableList(casts);
  }

  private List<Assignment> toAssignments(RowType rowType) {
    final List<ParquetColumn> childParquetColumns = JavaConverters.seqAsJavaList(parquetColumn.children());
    Preconditions.checkState(rowType.size() == childParquetColumns.size());
    final List<Assignment> list = new ArrayList<>();
    for (int i = 0; i < rowType.size(); i++) {
      final String name = rowType.getNames().get(i);
      final Type type = rowType.getChildren().get(i);
      list.add(new Assignment(name,
          new HiveColumnHandle(name, ColumnType.REGULAR, type, type, Collections.emptyList())));
    }
    return list;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    throw new UnsupportedOperationException();
  }

  public void initBatch(StructType partitionColumns, InternalRow partitionValues) {
    // TODO: row index support.
    this.partitionColumns = partitionColumns;
    this.partitionValues = partitionValues;

    final StructField[] fields = this.partitionColumns.fields();
    partitionVectors = new ConstantColumnVector[fields.length];
    for (int i = 0; i < fields.length; i++) {
      partitionVectors[i] = new ConstantColumnVector(capacity, fields[i].dataType());
      ColumnVectorUtils.populate(partitionVectors[i], partitionValues, i);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (returnColumnarBatch) {
      return nextBatch();
    }
    if (columnarBatch == null || rowOffsetInBatch == columnarBatch.numRows()) {
      if (!nextBatch()) {
        return false;
      }
    }
    ++rowOffsetInBatch;
    return true;
  }

  @Override
  public Void getCurrentKey() throws IOException, InterruptedException {
    return null;
  }

  @Override
  public Object getCurrentValue() throws IOException, InterruptedException {
    if (returnColumnarBatch) {
      return columnarBatch;
    }
    final InternalRow row = columnarBatch.getRow(rowOffsetInBatch - 1);
    return row;
  }

  private boolean nextBatch() throws IOException, InterruptedException {
    if (arrowDataStructVector != null) {
      arrowDataStructVector.close();
      arrowDataStructVector = null;
    }
    if (!veloxOutIterator.hasNext()) {
      return false;
    }
    try (final RowVector rowVector = veloxOutIterator.next()) {
      Preconditions.checkState(rowVector.getSize() <= capacity, "The row-vector returned by Velox exceeds the size limit: " + rowVector.getSize());
      // Exports to Arrow struct vector rather than a vector schema root because the latter
      // doesn't preserve top-level row count if there are no children.
      arrowDataStructVector = (StructVector) Arrow.toArrowVector(bufferAllocator, rowVector);
      Preconditions.checkState(arrowDataStructVector.getNullCount() == 0);
      final ArrowColumnVector[] dataVectors = arrowDataStructVector.getChildrenFromFields().stream()
          .map(ArrowColumnVector::new)
          .toArray(ArrowColumnVector[]::new);
      columnarBatch =
          new ColumnarBatch(
              Stream.concat(Arrays.stream(dataVectors), Arrays.stream(partitionVectors))
                  .toArray(ColumnVector[]::new), arrowDataStructVector.getValueCount());
      rowsReturned += columnarBatch.numRows();
      rowOffsetInBatch = 0;
      return true;
    }
  }

  @Override
  public float getProgress() {
    return (float) rowsReturned / totalRowCount;
  }

  /**
   * Can be called before any rows are returned to enable returning columnar batches directly.
   */
  public void enableReturningBatches() {
    returnColumnarBatch = true;
  }

  @Override
  public void close() throws IOException {
    if (initialized) {
      if (partitionVectors != null) {
        for (ColumnVector partitionVector : partitionVectors) {
          partitionVector.close();
        }
        partitionVectors = null;
      }
      if (arrowDataStructVector != null) {
        arrowDataStructVector.close();
        arrowDataStructVector = null;
      }
      task.close();
      session.close();
      memoryManager.close();
      bufferAllocator.close();
      Preconditions.checkState(listener.currentBytes() == 0);
    }
  }

  private static <K, V> Map<K, Set<V>> toSetMultiMap(Map<K, V> map) {
    Map<K, Set<V>> setMultiMap = new HashMap<>();
    for (Map.Entry<K, V> entry : map.entrySet()) {
      Set<V> set = new HashSet<>();
      set.add(entry.getValue());
      setMultiMap.put(entry.getKey(), Collections.unmodifiableSet(set));
    }
    return Collections.unmodifiableMap(setMultiMap);
  }

  /**
   * @param readSupportClass to instantiate
   * @return the configured read support
   */
  private static <T> ReadSupport<T> getReadSupportInstance(
      Class<? extends ReadSupport<T>> readSupportClass){
    try {
      return readSupportClass.getConstructor().newInstance();
    } catch (InstantiationException | IllegalAccessException |
             NoSuchMethodException | InvocationTargetException e) {
      throw new BadConfigurationException("could not instantiate read support class", e);
    }
  }

  @SuppressWarnings("unchecked")
  private Class<? extends ReadSupport<Object>> getReadSupportClass(Configuration configuration) {
    return (Class<? extends ReadSupport<Object>>) ConfigurationUtil.getClassFromConfig(configuration,
        ParquetInputFormat.READ_SUPPORT_CLASS, ReadSupport.class);
  }

  private DataType toLowerCase(DataType type) {
    if (type instanceof StructType) {
      final StructType structType = (StructType) type;
      return new StructType(
          Arrays.stream(structType.fields()).map(
                  field -> new StructField(
                      field.name().toLowerCase(), toLowerCase(field.dataType()), field.nullable(), field.metadata()))
              .toArray(StructField[]::new));
    }
    if (type instanceof ArrayType) {
      final ArrayType arrayType = (ArrayType) type;
      return new ArrayType(toLowerCase(arrayType.elementType()), arrayType.containsNull());
    }
    if (type instanceof MapType) {
      final MapType mapType = (MapType) type;
      return new MapType(toLowerCase(mapType.keyType()), toLowerCase(mapType.valueType()), mapType.valueContainsNull());
    }
    return type;
  }
}
