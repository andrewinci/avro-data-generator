package com.github.andrewinci.generators

import com.github.andrewinci.core.AvroFieldGenerator
import com.github.andrewinci.core.FieldGeneratorException
import com.github.andrewinci.generators.helpers.AvroFieldGeneratorLeaf
import com.github.andrewinci.generators.helpers.AvroFieldGeneratorNode
import com.github.andrewinci.generators.helpers.Compose.compose
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.andrewinci.generators.helpers.AvroFieldGeneratorHelper.avroEnumPicker
import org.apache.avro.LogicalTypes.Date
import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.LogicalTypes.LocalTimestampMicros
import org.apache.avro.LogicalTypes.LocalTimestampMillis
import org.apache.avro.LogicalTypes.TimeMicros
import org.apache.avro.LogicalTypes.TimeMillis
import org.apache.avro.LogicalTypes.TimestampMicros
import org.apache.avro.LogicalTypes.TimestampMillis
import org.apache.avro.Schema.Type
import org.apache.avro.data.TimeConversions.DateConversion
import org.apache.avro.data.TimeConversions.LocalTimestampMicrosConversion
import org.apache.avro.data.TimeConversions.LocalTimestampMillisConversion
import org.apache.avro.data.TimeConversions.TimeMicrosConversion
import org.apache.avro.data.TimeConversions.TimeMillisConversion
import org.apache.avro.data.TimeConversions.TimestampMicrosConversion
import org.apache.avro.data.TimeConversions.TimestampMillisConversion
import org.apache.avro.Conversions
import org.apache.avro.LogicalType
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema

import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.UUID
import scala.util.Try

object AvroFieldGen {
  import com.github.andrewinci.core.AvroFieldGenerator

  type FieldNameToAvroGen = (String, (Schema) => Either[FieldGeneratorException, Any])

  /** Given a map field path (i.e. field1.nested1.nested2...) to field generator, build
    * an avro field generator
    * @param map map field path to field gen
    */
  def fromMap(map: FieldNameToAvroGen*): AvroFieldGenerator = map
    .map(m => m._1.split('.') -> m._2)
    .map(m => m._1.foldRight[AvroFieldGenerator](AvroFieldGeneratorLeaf(m._2))((a, b) => AvroFieldGeneratorNode(a, b)))
    .reduce((g1, g2) => compose(g1, g2))

  def fromJson(jsonString: String): Either[Throwable, AvroFieldGenerator] = Try {
    new ObjectMapper().readTree(jsonString)
  }.map(j => JsonAvroFieldGen(j)).toEither

  def constantFieldsGen(
      str: String = "",
      int: Int = 0,
      decimal: BigDecimal = 0.1,
      boolean: Boolean = true,
      bytes: Array[Byte] = Array[Byte](1.toByte, 2.toByte, 3, 4),
      uUID: UUID = UUID.randomUUID(),
      localDate: LocalDate = LocalDate.now(),
      localTime: LocalTime = LocalTime.now(),
      localDateTime: LocalDateTime = LocalDateTime.now(),
      instant: Instant = Instant.now()
  ) = new ConstAvroFieldGen(str, int, decimal, boolean, bytes, uUID, localDate, localTime, localDateTime, instant)
}

abstract class AvroFieldGen extends AvroFieldGenerator {

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
