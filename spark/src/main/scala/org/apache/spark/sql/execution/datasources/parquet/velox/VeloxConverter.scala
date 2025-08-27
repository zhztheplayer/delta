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

import java.math.{BigDecimal => JavaBigDecimal}
import java.sql.{Date, Timestamp}
import java.time._

import scala.collection.JavaConverters._
import scala.collection.mutable

import io.github.zhztheplayer.velox4j.`type`._
import io.github.zhztheplayer.velox4j.arrow.Arrow
import io.github.zhztheplayer.velox4j.exception.VeloxException
import io.github.zhztheplayer.velox4j.expression.{CallTypedExpr, ConstantTypedExpr, DereferenceTypedExpr, FieldAccessTypedExpr, TypedExpr}
import io.github.zhztheplayer.velox4j.session.Session
import io.github.zhztheplayer.velox4j.variant._
import org.apache.arrow.memory.BufferAllocator

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.util.DateTimeUtils.instantToMicros
import org.apache.spark.sql.sources._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.unsafe.types.UTF8String

class VeloxConverter private(
  session: Session, bufferAllocator: BufferAllocator, timeZoneId: String,
  isCaseSensitive: Boolean) {
  def toVeloxRowType(sparkSchema: org.apache.spark.sql.types.StructType): RowType = {
    val arrowSchema = ArrowUtils.toArrowSchema(
      sparkSchema,
      timeZoneId,
      errorOnDuplicatedFieldNames = true,
      largeVarTypes = false)
    Arrow.fromArrowSchema(bufferAllocator, arrowSchema)
  }

  def toVeloxType(sparkType: org.apache.spark.sql.types.DataType, nullable: Boolean): Type = {
    val arrowField = ArrowUtils.toArrowField("", sparkType, nullable, timeZoneId)
    Arrow.fromArrowField(bufferAllocator, arrowField)
  }

  def filterToVeloxExpr(scanRowType: RowType, filter: Filter): TypedExpr = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper

    def buildLookUp(names: Seq[String], t: Type, access: TypedExpr,
      attributeToExprLookUpBuilder: mutable.ArrayBuffer[(String, TypedExpr)]): Unit = t match {
      case st: RowType =>
        for (i <- 0 until st.size()) {
          buildLookUp(
            names ++ Seq(st.getNames.get(i)), st.getChildren.get(i),
            DereferenceTypedExpr.create(access, i), attributeToExprLookUpBuilder)
        }
        attributeToExprLookUpBuilder += names.quoted -> access
      case otherType =>
        attributeToExprLookUpBuilder += names.quoted -> access
    }

    val builder = mutable.ArrayBuffer[(String, TypedExpr)]()

    for (i <- 0 until scanRowType.size()) {
      val childName = scanRowType.getNames.get(i)
      val childType = scanRowType.getChildren.get(i)
      buildLookUp(Seq(childName),
        childType, FieldAccessTypedExpr.create(childType, childName), builder)
    }

    val attributeToExprLookUp = builder
      .groupBy(_._1)
      .map {
        case (key, values) =>
          require(values.size == 1, s"Duplicated field in schema: $key")
          key -> values.head._2
      }

    def fieldAccess(attribute: String): TypedExpr = {
      val attr = if (isCaseSensitive) {
        attribute
      } else {
        attribute.toLowerCase()
      }
      checkAttribute(attr)
      attributeToExprLookUp(attr)
    }

    def fieldType(attribute: String): Type = {
      val attr = if (isCaseSensitive) {
        attribute
      } else {
        attribute.toLowerCase()
      }
      checkAttribute(attr)
      attributeToExprLookUp(attr).getReturnType
    }

    def checkAttribute(attribute: String): Unit = {
      if (!attributeToExprLookUp.contains(attribute)) {
        throw new VeloxException(
          s"Attribute $attribute doesn't exist in the lookup: $attributeToExprLookUp")
      }
    }

    filter match {
      case EqualTo(attribute, value) =>
        new CallTypedExpr(
          new BooleanType(),
          Seq(fieldAccess(attribute), constant(
            fieldType(attribute), value)).asJava,
          "equalto")
      case EqualNullSafe(attribute, value) =>
        new CallTypedExpr(
          new BooleanType(),
          Seq(fieldAccess(attribute), constant(
            fieldType(attribute), value)).asJava,
          "equalnullsafe")
      case GreaterThan(attribute, value) =>
        new CallTypedExpr(
          new BooleanType(),
          Seq(fieldAccess(attribute), constant(
            fieldType(attribute), value)).asJava,
          "greaterthan")
      case GreaterThanOrEqual(attribute, value) =>
        new CallTypedExpr(
          new BooleanType(),
          Seq(fieldAccess(attribute), constant(
            fieldType(attribute), value)).asJava,
          "greaterthanorequal")
      case LessThan(attribute, value) =>
        new CallTypedExpr(
          new BooleanType(),
          Seq(fieldAccess(attribute), constant(
            fieldType(attribute), value)).asJava,
          "lessthan")
      case LessThanOrEqual(attribute, value) =>
        new CallTypedExpr(
          new BooleanType(),
          Seq(fieldAccess(attribute), constant(
            fieldType(attribute), value)).asJava,
          "lessthanorequal")
      case In(attribute, values) =>
        new CallTypedExpr(
          new BooleanType(),
          Seq(fieldAccess(attribute), constant(
            ArrayType.create(fieldType(attribute)), values)).asJava,
          "in")
      case IsNull(attribute) =>
        new CallTypedExpr(
          new BooleanType(),
          Seq(fieldAccess(attribute)).asJava,
          "isnull")
      case IsNotNull(attribute) =>
        new CallTypedExpr(
          new BooleanType(),
          Seq(fieldAccess(attribute)).asJava,
          "isnotnull")
      case And(left, right) =>
        new CallTypedExpr(
          new BooleanType(),
          Seq(filterToVeloxExpr(scanRowType, left), filterToVeloxExpr(scanRowType, right)).asJava,
          "and")
      case Or(left, right) =>
        new CallTypedExpr(
          new BooleanType(),
          Seq(filterToVeloxExpr(scanRowType, left), filterToVeloxExpr(scanRowType, right)).asJava,
          "or")
      case Not(child) =>
        new CallTypedExpr(
          new BooleanType(),
          Seq(filterToVeloxExpr(scanRowType, child)).asJava,
          "not")
      case StringStartsWith(attribute, value) =>
        new CallTypedExpr(
          new BooleanType(),
          Seq(fieldAccess(attribute), constant(
            fieldType(attribute), value)).asJava,
          "startswith")
      case StringEndsWith(attribute, value) =>
        new CallTypedExpr(
          new BooleanType(),
          Seq(fieldAccess(attribute), constant(
            fieldType(attribute), value)).asJava,
          "endswith")
      case StringContains(attribute, value) =>
        new CallTypedExpr(
          new BooleanType(),
          Seq(fieldAccess(attribute), constant(
            fieldType(attribute), value)).asJava,
          "contains")
      case AlwaysTrue() => constant(new BooleanType(), true)
      case AlwaysFalse() => constant(new BooleanType(), false)
    }
  }

  private def constant(veloxType: Type, value: Any): TypedExpr = {
    val variantValue = toVariant(veloxType, value)
    val variantVector = session.variantOps().toVector(veloxType, variantValue)
    ConstantTypedExpr.create(variantVector)
  }

  private def toVariant(veloxType: Type, value: Any): Variant = (veloxType, value) match {
    // Numeric primitives
    case (_: IntegerType, null) => new IntegerValue(null)
    case (_: IntegerType, i: java.lang.Integer) => new IntegerValue(i)
    case (_: BigIntType, null) => new BigIntValue(null)
    case (_: BigIntType, l: java.lang.Long) => new BigIntValue(l)
    case (_: DoubleType, null) => new DoubleValue(null)
    case (_: DoubleType, d: java.lang.Double) => new DoubleValue(d)
    case (_: RealType, null) => new RealValue(null)
    case (_: RealType, f: java.lang.Float) => new RealValue(f)
    case (_: TinyIntType, null) => new TinyIntValue(null)
    case (_: TinyIntType, b: java.lang.Byte) => new TinyIntValue(b.intValue())
    case (_: SmallIntType, null) => new SmallIntValue(null)
    case (_: SmallIntType, s: java.lang.Short) => new SmallIntValue(s.intValue())

    // Booleans
    case (_: BooleanType, null) => new BooleanValue(null)
    case (_: BooleanType, b: java.lang.Boolean) => new BooleanValue(b)

    // Strings (handle several input shapes)
    case (_: VarCharType, null) => new VarCharValue(null)
    case (_: VarCharType, s: String) =>
      new VarCharValue(UTF8String.fromString(s).toString)
    case (_: VarCharType, u: UTF8String) => new VarCharValue(u.toString)
    case (_: VarCharType, c: Char) =>
      new VarCharValue(UTF8String.fromString(c.toString).toString)
    case (_: VarCharType, ac: Array[Char]) =>
      new VarCharValue(UTF8String.fromString(new String(ac)).toString)

    // Binary
    case (_: VarbinaryType, null) => VarBinaryValue.create(null)
    case (_: VarbinaryType, bytes: Array[Byte]) => VarBinaryValue.create(bytes)

    // Decimal (support scala/java BigDecimal and Spark Decimal)
    case (dt: DecimalType, decimal) =>
      val dcValue: Option[org.apache.spark.sql.types.Decimal] = decimal match {
        case null => None
        case bd: BigDecimal => Some(org.apache.spark.sql.types.Decimal(bd))
        case jd: JavaBigDecimal => Some(org.apache.spark.sql.types.Decimal(jd))
        case dec: org.apache.spark.sql.types.Decimal => Some(dec)
      }
      if (dt.getPrecision <= 18) {
        new BigIntValue(dcValue.map(_.toLong.asInstanceOf[java.lang.Long]).orNull)
      } else {
        new HugeIntValue(dcValue.map(_.toJavaBigInteger).orNull)
      }

    // Dates
    case (_: DateType, null) =>
      new IntegerValue(null)
    case (_: DateType, ld: LocalDate) =>
      new IntegerValue(ld.toEpochDay.toInt)
    case (_: DateType, d: Date) =>
      new IntegerValue(DateTimeUtils.fromJavaDate(d))

    // Timestamps (with time zone)
    case (_: TimestampType, null) =>
      TimestampValue.createNull()
    case (_: TimestampType, i: Instant) =>
      val mircoSeconds = instantToMicros(i)
      TimestampValue.create(
        mircoSeconds / 1000000, (mircoSeconds % 1000000) * 1000)
    case (_: TimestampType, ts: Timestamp) =>
      val mircoSeconds = DateTimeUtils.fromJavaTimestamp(ts)
      TimestampValue.create(
        mircoSeconds / 1000000, (mircoSeconds % 1000000) * 1000)

    // Arrays (Scala/Java/WrappedArray) -> ArrayType
    case (_: ArrayType, null) =>
      new ArrayValue(null)
    case (at: ArrayType, arr: Array[_]) =>
      val childType = at.getChildren.get(0)
      val childrenValues = arr.toStream
        .map(childValue => {
          toVariant(childType, childValue)
        })
        .asJava
      new ArrayValue(childrenValues)
    case (t, v) =>
      // No match — surface a precise error to help debugging.
      throw new IllegalArgumentException(
        s"Unsupported (value=${Option(v).map(_.getClass.getName).orNull}) for dataType=$t in " +
          s"toVariant")
  }
}

object VeloxConverter {
  def of(session: Session, bufferAllocator: BufferAllocator, timeZoneId: String,
         isCaseSensitive: Boolean): VeloxConverter = {
    new VeloxConverter(session, bufferAllocator, timeZoneId, isCaseSensitive)
  }
}
