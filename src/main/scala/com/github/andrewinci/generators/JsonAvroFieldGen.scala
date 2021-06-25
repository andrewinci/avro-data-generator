package com.github.andrewinci.generators

import com.fasterxml.jackson.databind.JsonNode
import com.github.andrewinci.core.AvroFieldGenerator
import com.github.andrewinci.core.FieldGeneratorException
import com.github.andrewinci.generators.helpers.AvroFieldGeneratorHelper.avroEnumPicker
import com.github.andrewinci.generators.helpers.AvroFieldGeneratorLeaf
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
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

import java.math.BigDecimal
import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.Base64
import java.util.UUID
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class JsonAvroFieldGen(json: JsonNode) extends AvroFieldGen {

  def unsupportedField(field: String) = Left(new FieldGeneratorException(s"Field $field not supported"))
  def invalidField(field: String) = Left(new FieldGeneratorException(s"Invalid $field type"))

  override def generate(schema: Schema): Either[FieldGeneratorException, Any] = EmptyAvroFieldGen.generate(schema)

  override def getGenerator(fieldName: String): Option[AvroFieldGenerator] = {
    Option(if (json.isArray) json.get(fieldName.toInt) else json.get(fieldName))
      .map(field => {
        if (field.isObject || field.isArray) JsonAvroFieldGen(field)
        else
          AvroFieldGeneratorLeaf(schema =>
            Option(schema.getLogicalType) match {
              // not a logical type
              case None => handlePrimitiveType(field, schema)
              // handle logical types
              case Some(logicalType) => handleLogicalType(field, schema, logicalType)
            }
          )
      })
  }

  private def handlePrimitiveType(field: JsonNode, schema: Schema): Either[FieldGeneratorException, Any] =
    schema.getType match {
      case Type.RECORD | Type.ARRAY | Type.MAP | Type.UNION | Type.FIXED => unableToGenerateComplexTypeException(schema)
      // only primitive types are supported
      case Type.BYTES   => parseBase64(field.textValue())
      case Type.ENUM    => avroEnumPicker(schema)(_.find(field.textValue() == _))
      case Type.INT     => Right(field.numberValue().intValue())
      case Type.LONG    => Right(field.numberValue().longValue())
      case Type.DOUBLE  => Right(field.numberValue().doubleValue())
      case Type.FLOAT   => Right(field.numberValue().floatValue())
      case Type.STRING  => Right(field.textValue())
      case Type.BOOLEAN => Right(field.booleanValue())
      case Type.NULL    => Right(null)
    }

  private def parseBase64(v: String) =
    Try { ByteBuffer.wrap(Base64.getDecoder.decode(v)) } match {
      case Success(v) => Right(v)
      case Failure(e) => Left(new FieldGeneratorException(s"Unable to parser the field $v to BYTES", Some(e)))
    }

  private def handleLogicalType(
      field: JsonNode,
      schema: Schema,
      logType: LogicalType
  ): Either[FieldGeneratorException, Any] =
    Try {
      logType match {
        case _: Decimal              => getDecimal(BigDecimal.valueOf(field.numberValue().doubleValue()), schema, logType)
        case _: Date                 => getDate(LocalDate.parse(field.textValue()), schema, logType)
        case _: TimeMillis           => getTimeMillis(LocalTime.parse(field.textValue()), schema, logType)
        case _: TimeMicros           => getTimeMicros(LocalTime.parse(field.textValue()), schema, logType)
        case _: TimestampMillis      => getTimestampMicros(Instant.parse(field.textValue()), schema, logType)
        case _: TimestampMicros      => getTimestampMicros(Instant.parse(field.textValue()), schema, logType)
        case _: LocalTimestampMillis => getLocalTimestampMicros(LocalDateTime.parse(field.textValue()), schema, logType)
        case _: LocalTimestampMicros => getLocalTimestampMicros(LocalDateTime.parse(field.textValue()), schema, logType)
        case _ =>
          if (logType == LogicalTypes.uuid()) getUUID(UUID.fromString(field.textValue()), schema, logType)
          else null
      }
    } match {
      case Success(null)     => unableToGenerateFieldForLogicalTypeException(schema, logType)
      case Success(Right(v)) => Right(v)
      case Failure(e)        => unableToGenerateFieldForLogicalTypeException(schema, logType, Some(e))
    }
}

object JsonAvroFieldGen {
  def apply(jsonNode: JsonNode) = new JsonAvroFieldGen(jsonNode)
}
