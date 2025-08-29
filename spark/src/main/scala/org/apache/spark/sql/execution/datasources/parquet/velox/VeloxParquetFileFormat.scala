/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.parquet.velox

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.execution.datasources.parquet.ParquetUtils.{hasFieldIds, isBatchReadSupported}
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, FileFormat, PartitionedFile, RecordReaderIterator}
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnVector
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters._
import scala.collection.Seq;

class VeloxParquetFileFormat extends ParquetFileFormat {
  override def vectorTypes(
      requiredSchema: StructType,
      partitionSchema: StructType,
      sqlConf: SQLConf): Option[Seq[String]] = {
    val resultSchema = StructType(partitionSchema.fields ++ requiredSchema.fields)
    val enableVeloxVectorizedReader: Boolean =
      VeloxParquetFileFormat.isVeloxBatchReadSupportedForSchema(sqlConf, resultSchema)
    if (enableVeloxVectorizedReader) {
      return Option(
        Seq.fill(requiredSchema.fields.length)(classOf[ColumnVector].getName) ++ Seq.fill(
          partitionSchema.fields.length) {
          classOf[ConstantColumnVector].getName
        })
    }
    super.vectorTypes(requiredSchema, partitionSchema, sqlConf)
  }

  /**
   * Returns whether the reader can return the rows as batch or not.
   */
  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    val sqlConf = sparkSession.sessionState.conf
    val supportBatch = VeloxParquetFileFormat.isVeloxBatchReadSupportedForSchema(sqlConf, schema)
    if (supportBatch) {
      return true
    }
    super.supportBatch(sparkSession, schema)
  }

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA, requiredSchema.json)
    hadoopConf.set(ParquetWriteSupport.SPARK_ROW_SCHEMA, requiredSchema.json)
    hadoopConf.set(
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      sparkSession.sessionState.conf.sessionLocalTimeZone)
    hadoopConf.setBoolean(
      SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key,
      sparkSession.sessionState.conf.nestedSchemaPruningEnabled)
    hadoopConf.setBoolean(
      SQLConf.CASE_SENSITIVE.key,
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    // Sets flags for `ParquetToSparkSchemaConverter`
    hadoopConf.setBoolean(
      SQLConf.PARQUET_BINARY_AS_STRING.key,
      sparkSession.sessionState.conf.isParquetBinaryAsString)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      sparkSession.sessionState.conf.isParquetINT96AsTimestamp)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED.key,
      sparkSession.sessionState.conf.parquetInferTimestampNTZEnabled)
    hadoopConf.setBoolean(
      SQLConf.LEGACY_PARQUET_NANOS_AS_LONG.key,
      sparkSession.sessionState.conf.legacyParquetNanosAsLong)

    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    val resultSchema = StructType(partitionSchema.fields ++ requiredSchema.fields)
    val sqlConf = sparkSession.sessionState.conf
    val enableOffHeapColumnVector = sqlConf.offHeapColumnVectorEnabled
    val enableVectorizedReader: Boolean = sqlConf.parquetVectorizedReaderEnabled
    val enableVeloxVectorizedReader: Boolean =
      VeloxParquetFileFormat.isVeloxBatchReadSupportedForSchema(sqlConf, requiredSchema)
    val enableRecordFilter: Boolean = sqlConf.parquetRecordFilterEnabled
    val timestampConversion: Boolean = sqlConf.isParquetINT96TimestampConversion
    val capacity = sqlConf.parquetVectorizedReaderBatchSize
    val enableParquetFilterPushDown: Boolean = sqlConf.parquetFilterPushDown
    val pushDownDate = sqlConf.parquetFilterPushDownDate
    val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
    val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
    val pushDownStringPredicate = sqlConf.parquetFilterPushDownStringPredicate
    val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
    val isCaseSensitive = sqlConf.caseSensitiveAnalysis
    val parquetOptions = new ParquetOptions(options, sparkSession.sessionState.conf)
    val datetimeRebaseModeInRead = parquetOptions.datetimeRebaseModeInRead
    val int96RebaseModeInRead = parquetOptions.int96RebaseModeInRead

    // Should always be set by FileSourceScanExec creating this.
    // Check conf before checking option, to allow working around an issue by changing conf.
    val returningBatch = enableVeloxVectorizedReader &&
      options
        .getOrElse(
          FileFormat.OPTION_RETURNING_BATCH,
          throw new IllegalArgumentException(
            "OPTION_RETURNING_BATCH should always be set for ParquetFileFormat. " +
              "To workaround this issue, set spark.sql.parquet.enableVectorizedReader=false."))
        .equals("true")
    if (returningBatch) {
      // If the passed option said that we are to return batches, we need to also be able to
      // do this based on config and resultSchema.
      assert(supportBatch(sparkSession, resultSchema))
    }

    if (enableVectorizedReader && enableVeloxVectorizedReader) {
      return (file: PartitionedFile) => {
        assert(file.partitionValues.numFields == partitionSchema.size)

        val filePath = file.toPath
        val split = new FileSplit(filePath, file.start, file.length, Array.empty[String])

        val sharedConf = broadcastedHadoopConf.value.value

        val fileFooter = if (enableVectorizedReader) {
          // When there are vectorized reads, we can avoid reading the footer twice by reading
          // all row groups in advance and filter row groups according to filters that require
          // push down (no need to read the footer metadata again).
          ParquetFooterReader.readFooter(sharedConf, file, ParquetFooterReader.WITH_ROW_GROUPS)
        } else {
          ParquetFooterReader.readFooter(sharedConf, file, ParquetFooterReader.SKIP_ROW_GROUPS)
        }

        val footerFileMetaData = fileFooter.getFileMetaData
        val datetimeRebaseSpec = DataSourceUtils.datetimeRebaseSpec(
          footerFileMetaData.getKeyValueMetaData.get,
          datetimeRebaseModeInRead)
        val int96RebaseSpec = DataSourceUtils.int96RebaseSpec(
          footerFileMetaData.getKeyValueMetaData.get,
          int96RebaseModeInRead)

        def isCreatedByParquetMr: Boolean =
          footerFileMetaData.getCreatedBy().startsWith("parquet-mr")
        val convertTz =
          if (timestampConversion && !isCreatedByParquetMr) {
            Some(DateTimeUtils.getZoneId(sharedConf.get(SQLConf.SESSION_LOCAL_TIMEZONE.key)))
          } else {
            None
          }

        val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
        val hadoopAttemptContext =
          new TaskAttemptContextImpl(broadcastedHadoopConf.value.value, attemptId)

        val taskContext = Option(TaskContext.get())

        val veloxVectorizedReader = new VeloxVectorizedParquetRecordReader(
          convertTz.orNull,
          datetimeRebaseSpec.mode.toString,
          datetimeRebaseSpec.timeZone,
          int96RebaseSpec.mode.toString,
          int96RebaseSpec.timeZone,
          enableOffHeapColumnVector && taskContext.isDefined,
          isCaseSensitive,
          capacity)
        val iter = new RecordReaderIterator(veloxVectorizedReader)
        try {
          veloxVectorizedReader.initialize(
            split,
            hadoopAttemptContext,
            Option.apply(fileFooter),
            filters.asJava)
          logDebug(s"Appending $partitionSchema ${file.partitionValues}")
          veloxVectorizedReader.initBatch(partitionSchema, file.partitionValues)
          if (returningBatch) {
            veloxVectorizedReader.enableReturningBatches()
          }
          // UnsafeRowParquetRecordReader appends the columns internally to avoid another copy.
          iter.asInstanceOf[Iterator[InternalRow]]
        } catch {
          case e: Throwable =>
            // SPARK-23457: In case there is an exception in initialization, close the iterator to
            // avoid leaking resources.
            iter.close()
            throw e
        }
      }
    }
    super.buildReaderWithPartitionValues(
      sparkSession,
      dataSchema,
      partitionSchema,
      requiredSchema,
      filters,
      options,
      hadoopConf)
  }
}

object VeloxParquetFileFormat {

  def isVeloxBatchReadSupportedForSchema(sqlConf: SQLConf, schema: StructType): Boolean =
    sqlConf.parquetVectorizedReaderEnabled &&
      sqlConf.getConf(DeltaSQLConf.VELOX_PARQUET_VECTORIZED_READER_ENABLED) &&
      !hasFieldIds(schema) && schema.forall(f => isVeloxBatchReadSupported(sqlConf, f))

  def isVeloxBatchReadSupported(sqlConf: SQLConf, f: StructField): Boolean = {
    f.dataType match {
      case _: TimestampNTZType => // Velox doesn't support it.
        false
      case _: DayTimeIntervalType => // Velox's interval type is in millisecond precision.
        false
      case _: AtomicType =>
        true
      case at: ArrayType =>
        sqlConf.parquetVectorizedReaderNestedColumnEnabled &&
        isBatchReadSupported(sqlConf, at.elementType)
      case mt: MapType =>
        sqlConf.parquetVectorizedReaderNestedColumnEnabled &&
        isBatchReadSupported(sqlConf, mt.keyType) &&
        isBatchReadSupported(sqlConf, mt.valueType)
      case st: StructType =>
        sqlConf.parquetVectorizedReaderNestedColumnEnabled &&
        st.fields.forall(f => isBatchReadSupported(sqlConf, f.dataType))
      case udt: UserDefinedType[_] =>
        isBatchReadSupported(sqlConf, udt.sqlType)
      case _ =>
        false
    }
  }
}
