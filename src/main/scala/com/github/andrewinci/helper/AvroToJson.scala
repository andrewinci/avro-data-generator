package com.github.andrewinci.helper

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.apache.avro.Conversions
import org.apache.avro.LogicalType
import org.apache.avro.Schema
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
import org.apache.avro.generic.GenericRecord

import scala.collection.convert.Wrappers.SeqWrapper
import scala.collection.JavaConverters._
import java.nio.ByteBuffer
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.Base64
import scala.util.Success
import scala.util.Try

object AvroToJson {

  private lazy val jsonFactory = JsonNodeFactory.instance

  def parseRecord(record: GenericRecord): JsonNode = {
    val schema = record.getSchema
    val json = jsonFactory.objectNode()
    schema.getFields.forEach(f => {
      json.set(f.name(), parse(record.get(f.name()), f.schema()))
    })
    json
  }

  def parseArray(value: SeqWrapper[Any], itemSchema: Schema): JsonNode = {
    val res = jsonFactory.arrayNode()
    value.forEach(o => res.add(parse(o, itemSchema)))
    res
  }

  private def parse(data: Any, schema: Schema): JsonNode = {
    Option(schema.getLogicalType) match {
      // not a logical type
      case None => handlePrimitiveType(data, schema)
      // handle logical types
      case Some(logicalType) => handleLogicalType(data, schema, logicalType)
    }
  }

  private def parseUnion(data: Any, schema: Schema): JsonNode =
    schema.getTypes
      .iterator()
      .asScala
      .map(s => {
        Try { parse(data, s) } match {
          case Success(value) => Some(value)
          case _              => None
        }
      })
      .find(_.nonEmpty)
      .flatten
      .get

  private def handlePrimitiveType(data: Any, schema: Schema): JsonNode =
    schema.getType match {
      case Type.RECORD  => parseRecord(data.asInstanceOf[GenericRecord])
      case Type.UNION   => parseUnion(data, schema)
      case Type.ARRAY   => parseArray(data.asInstanceOf[SeqWrapper[Any]], schema.getElementType)
      case Type.ENUM    => jsonFactory.textNode(data.asInstanceOf[String])
      case Type.STRING  => jsonFactory.textNode(data.asInstanceOf[String])
      case Type.INT     => jsonFactory.numberNode(data.asInstanceOf[Int])
      case Type.LONG    => jsonFactory.numberNode(data.asInstanceOf[Long])
      case Type.FLOAT   => jsonFactory.numberNode(data.asInstanceOf[Float])
      case Type.DOUBLE  => jsonFactory.numberNode(data.asInstanceOf[Double])
      case Type.BOOLEAN => jsonFactory.booleanNode(data.asInstanceOf[Boolean])
      case Type.NULL    => jsonFactory.nullNode()
      //      case  Type.MAP | Type.FIXED =>
      case Type.BYTES => jsonFactory.textNode(Base64.getEncoder.encodeToString(data.asInstanceOf[ByteBuffer].array()))
      case _          => throw new Exception("Unsupported")
    }

  //
  private def handleLogicalType(data: Any, schema: Schema, logicalType: LogicalType): JsonNode = {
    logicalType match {
      case _: Decimal =>
        jsonFactory.numberNode(getDecimal(data.asInstanceOf[ByteBuffer], schema, logicalType))
      case _: Date =>
        jsonFactory.textNode(getDate(data.asInstanceOf[Int], schema, logicalType).toString)
      case _: TimeMillis =>
        jsonFactory.textNode(getTimeMillis(data.asInstanceOf[Int], schema, logicalType).toString)
      case _: TimeMicros =>
        jsonFactory.textNode(getTimeMicros(data.asInstanceOf[Long], schema, logicalType).toString)
      case _: TimestampMillis =>
        jsonFactory.textNode(getTimestampMicros(data.asInstanceOf[Long], schema, logicalType).toString)
      case _: TimestampMicros =>
        jsonFactory.textNode(getTimestampMicros(data.asInstanceOf[Long], schema, logicalType).toString)
      case _: LocalTimestampMillis =>
        jsonFactory.textNode(getLocalTimestampMicros(data.asInstanceOf[Long], schema, logicalType).toString)
      case _: LocalTimestampMicros =>
        jsonFactory.textNode(getLocalTimestampMicros(data.asInstanceOf[Long], schema, logicalType).toString)
      case _ =>
        jsonFactory.textNode(data.toString)
    }
  }

  def getDecimal(bigDecimal: ByteBuffer, schema: Schema, logicalType: LogicalType): java.math.BigDecimal =
    new Conversions.DecimalConversion().fromBytes(bigDecimal, schema, logicalType)

  def getDate(value: Int, schema: Schema, logicalType: LogicalType): LocalDate =
    new DateConversion().fromInt(value, schema, logicalType)

  def getTimeMillis(value: Int, schema: Schema, logicalType: LogicalType): LocalTime =
    new TimeMillisConversion().fromInt(value, schema, logicalType)

  def getTimeMicros(value: Long, schema: Schema, logicalType: LogicalType): LocalTime =
    new TimeMicrosConversion().fromLong(value, schema, logicalType)

  def getTimestampMillis(value: Long, schema: Schema, logicalType: LogicalType): Instant =
    new TimestampMillisConversion().fromLong(value, schema, logicalType)

  def getTimestampMicros(value: Long, schema: Schema, logicalType: LogicalType): Instant =
    new TimestampMicrosConversion().fromLong(value, schema, logicalType)

  def getLocalTimestampMillis(value: Long, schema: Schema, logicalType: LogicalType): LocalDateTime =
    new LocalTimestampMillisConversion().fromLong(value, schema, logicalType)

  def getLocalTimestampMicros(value: Long, schema: Schema, logicalType: LogicalType): LocalDateTime =
    new LocalTimestampMicrosConversion().fromLong(value, schema, logicalType)

}
