package com.github.andrewinci.generators

import com.github.andrewinci.core.AvroFieldGenerator
import com.github.andrewinci.core.FieldGeneratorException
import com.github.andrewinci.generators.helpers.AvroFieldGeneratorHelper.avroEnumPicker
import org.apache.avro.LogicalTypes.Date
import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.LogicalTypes.LocalTimestampMicros
import org.apache.avro.LogicalTypes.LocalTimestampMillis
import org.apache.avro.LogicalTypes.TimeMicros
import org.apache.avro.LogicalTypes.TimeMillis
import org.apache.avro.LogicalTypes.TimestampMicros
import org.apache.avro.LogicalTypes.TimestampMillis
import org.apache.avro.LogicalType
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema
import org.apache.avro.Schema.Type

import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.UUID

class ConstAvroFieldGen(
    constStr: String = "",
    constInt: Int = 0,
    constDecimal: BigDecimal = 0.1,
    constBoolean: Boolean = true,
    constBytes: Array[Byte] = Array[Byte](1.toByte, 2.toByte, 3, 4),
    constUUID: UUID = UUID.randomUUID(),
    constLocalDate: LocalDate = LocalDate.now(),
    constLocalTime: LocalTime = LocalTime.now(),
    constLocalDateTime: LocalDateTime = LocalDateTime.now(),
    constInstant: Instant = Instant.now()
) extends AvroFieldGen {

  override def getGenerator(fieldName: String): Option[AvroFieldGenerator] =
    // avoid infinite loop for array. Any array will have size 1
    if (fieldName == "1") None else Some(this)

  override def generate(schema: Schema): Either[FieldGeneratorException, Any] =
    Option(schema.getLogicalType) match {
      // not a logical type
      case None => handlePrimitiveType(schema)
      // handle logical types
      case Some(logicalType) => handleLogicalType(schema, logicalType)
    }

  private def handlePrimitiveType(schema: Schema) =
    schema.getType match {
      case Type.RECORD | Type.ARRAY | Type.MAP | Type.UNION | Type.FIXED => unableToGenerateComplexTypeException(schema)
      // only primitive types are supported
      case Type.ENUM    => avroEnumPicker(schema)(_.headOption)
      case Type.BYTES   => Right(ByteBuffer.wrap(constBytes))
      case Type.STRING  => Right(constStr)
      case Type.INT     => Right(constInt)
      case Type.LONG    => Right(constInt.toLong)
      case Type.FLOAT   => Right(constDecimal.toFloat)
      case Type.DOUBLE  => Right(constDecimal.toDouble)
      case Type.BOOLEAN => Right(constBoolean)
      case Type.NULL    => Right(null)
    }

  private def handleLogicalType(schema: Schema, logicalType: LogicalType) =
    logicalType match {
      case _: Decimal              => getDecimal(constDecimal.bigDecimal, schema, logicalType)
      case _: Date                 => getDate(constLocalDate, schema, logicalType)
      case _: TimeMillis           => getTimeMillis(constLocalTime, schema, logicalType)
      case _: TimeMicros           => getTimeMicros(constLocalTime, schema, logicalType)
      case _: TimestampMillis      => getTimestampMicros(constInstant, schema, logicalType)
      case _: TimestampMicros      => getTimestampMicros(constInstant, schema, logicalType)
      case _: LocalTimestampMillis => getLocalTimestampMicros(constLocalDateTime, schema, logicalType)
      case _: LocalTimestampMicros => getLocalTimestampMicros(constLocalDateTime, schema, logicalType)
      case _ =>
        if (logicalType == LogicalTypes.uuid()) getUUID(constUUID, schema, logicalType)
        else unableToGenerateFieldForLogicalTypeException(schema, logicalType)
    }
}
