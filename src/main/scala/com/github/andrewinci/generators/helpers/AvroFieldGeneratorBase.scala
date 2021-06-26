package com.github.andrewinci.generators.helpers

import com.github.andrewinci.core.AvroFieldGenerator
import com.github.andrewinci.core.FieldGeneratorException
import org.apache.avro.Conversions
import org.apache.avro.LogicalType
import org.apache.avro.Schema
import org.apache.avro.data.TimeConversions.DateConversion
import org.apache.avro.data.TimeConversions.LocalTimestampMicrosConversion
import org.apache.avro.data.TimeConversions.LocalTimestampMillisConversion
import org.apache.avro.data.TimeConversions.TimeMicrosConversion
import org.apache.avro.data.TimeConversions.TimeMillisConversion
import org.apache.avro.data.TimeConversions.TimestampMicrosConversion
import org.apache.avro.data.TimeConversions.TimestampMillisConversion

import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.UUID

abstract class AvroFieldGeneratorBase extends AvroFieldGenerator {

  // Example
  //  private def handlePrimitiveType(schema: Schema) =
  //    schema.getType match {
  //      case Type.RECORD | Type.ARRAY | Type.MAP | Type.UNION | Type.FIXED => unableToGenerateComplexTypeException(schema)
  //      // only primitive types are supported
  //      case Type.ENUM    => avroEnumPicker(schema)(_.headOption)
  //      case Type.BYTES   => Right(ByteBuffer.wrap(constBytes))
  //      case Type.STRING  => Right(constStr)
  //      case Type.INT     => Right(constInt)
  //      case Type.LONG    => Right(constInt.toLong)
  //      case Type.FLOAT   => Right(constDecimal.toFloat)
  //      case Type.DOUBLE  => Right(constDecimal.toDouble)
  //      case Type.BOOLEAN => Right(constBoolean)
  //      case Type.NULL    => Right(null)
  //    }
  //
  //  private def handleLogicalType(schema: Schema, logicalType: LogicalType) =
  //    logicalType match {
  //      case _: Decimal              => getDecimal(schema, logicalType)
  //      case _: Date                 => getDate(schema, logicalType)
  //      case _: TimeMillis           => getTimeMillis(schema, logicalType)
  //      case _: TimeMicros           => getTimeMicros(schema, logicalType)
  //      case _: TimestampMillis      => getTimestampMicros(schema, logicalType)
  //      case _: TimestampMicros      => getTimestampMicros(schema, logicalType)
  //      case _: LocalTimestampMillis => getLocalTimestampMicros(schema, logicalType)
  //      case _: LocalTimestampMicros => getLocalTimestampMicros(schema, logicalType)
  //      case _ =>
  //        if (logicalType == LogicalTypes.uuid()) getUUID(schema, logicalType)
  //        else unableToGenerateFieldForLogicalTypeException(schema, logicalType)
  //    }

  def unableToGenerateComplexTypeException(schema: Schema) =
    Left(new FieldGeneratorException(s"Unable to generate a field for complex type ${schema.getType.getName}"))

  def unableToGenerateFieldForLogicalTypeException(
      schema: Schema,
      logicalType: LogicalType,
      cause: Option[Throwable] = None
  ) =
    Left(
      new FieldGeneratorException(
        s"Unable to generate a field for logical type ${schema.getType.getName}:${logicalType.getName}",
        cause
      )
    )

  def getDecimal(bigDecimal: java.math.BigDecimal, schema: Schema, logicalType: LogicalType) =
    Right(new Conversions.DecimalConversion().toBytes(bigDecimal, schema, logicalType))

  def getDate(date: LocalDate, schema: Schema, logicalType: LogicalType) =
    Right(new DateConversion().toInt(date, schema, logicalType))

  def getTimeMillis(time: LocalTime, schema: Schema, logicalType: LogicalType) =
    Right(new TimeMillisConversion().toInt(time, schema, logicalType))

  def getTimeMicros(time: LocalTime, schema: Schema, logicalType: LogicalType) =
    Right(new TimeMicrosConversion().toLong(time, schema, logicalType))

  def getTimestampMillis(instant: Instant, schema: Schema, logicalType: LogicalType) =
    Right(new TimestampMillisConversion().toLong(instant, schema, logicalType))

  def getTimestampMicros(instant: Instant, schema: Schema, logicalType: LogicalType) =
    Right(new TimestampMicrosConversion().toLong(instant, schema, logicalType))

  def getLocalTimestampMillis(dateTime: LocalDateTime, schema: Schema, logicalType: LogicalType) =
    Right(new LocalTimestampMillisConversion().toLong(dateTime, schema, logicalType))

  def getLocalTimestampMicros(dateTime: LocalDateTime, schema: Schema, logicalType: LogicalType) =
    Right(new LocalTimestampMicrosConversion().toLong(dateTime, schema, logicalType))

  def getUUID(uuid: UUID, schema: Schema, logicalType: LogicalType) =
    Right(new Conversions.UUIDConversion().toCharSequence(uuid, schema, logicalType))
}
