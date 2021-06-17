package com.github.andrewinci

import com.github.andrewinci.core.AvroFieldGenerator
import com.github.andrewinci.core.AvroGeneratorException
import com.github.andrewinci.core.AvroRecordGenerator
import com.github.andrewinci.core.NotImplementedException
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

import scala.collection.JavaConverters._

class AvroGenerator(val fieldGenerator: AvroFieldGenerator) extends AvroRecordGenerator {

  /** Generate a random avro record
    *
    * @param schema the avro schema to use to generate the record
    * @return a random record with the provided schema
    */
  override def generateRecord(schema: Schema): Either[AvroGeneratorException, GenericRecord] =
    if (schema.getType != Type.RECORD) Left(new AvroGeneratorException("Only RECORD is supported"))
    else generateRecord(schema, fieldGenerator)

  private def validate[A](record: A, schema: Schema): Either[AvroGeneratorException, A] =
    if (new GenericData().validate(schema, record)) Right(record)
    else Left(new AvroGeneratorException(s"Invalid avro generated"))

  private def generateRecord(
      schema: Schema,
      fieldGenerator: AvroFieldGenerator
  ): Either[AvroGeneratorException, GenericRecord] = {
    val record = new GenericData.Record(schema)
    schema.getFields.forEach(field => {
      fieldGenerator.getGenerator(field.name()) match {
        case Some(gen) =>
          generateValue(field.schema(), gen) match {
            case Right(value) => record.put(field.name(), value)
            case Left(e)      => return Left(new AvroGeneratorException(s"Unable to set value for ${field.name()}", e))
          }
        case None => return Left(new AvroGeneratorException(s"No generator specified for field ${field.name()}"))
      }
    })
    validate(record, schema)
  }

  private def generateUnion(schema: Schema, fieldGenerator: AvroFieldGenerator): Either[AvroGeneratorException, Any] =
    schema.getTypes.asScala
      .map(generateValue(_, fieldGenerator))
      .map(_.flatMap(validate(_, schema)))
      .find(_.isRight)
      .getOrElse(Left(new AvroGeneratorException("Unable to build any value in the UNION")))

  private def generateArray(schema: Schema, fieldGenerator: AvroFieldGenerator): Either[AvroGeneratorException, Any] = {
    val res = Stream
      .from(0)
      .map(i => fieldGenerator.getGenerator(i.toString))
      .takeWhile(_.isDefined)
      .map(g => generateValue(schema.getElementType, g.get))
    if (res.exists(_.isLeft))
      Left(new AvroGeneratorException(s"Unable to generate ARRAY of ${schema.getElementType.getName}"))
    else Right(seqAsJavaList(res.map(_.right.get)))
  }

  private def generateValue(schema: Schema, fieldGenerator: AvroFieldGenerator): Either[AvroGeneratorException, Any] = {
    schema.getType match {
      // complex data types
      case Type.RECORD => generateRecord(schema, fieldGenerator)
      case Type.UNION  => generateUnion(schema, fieldGenerator)
      case Type.ARRAY  => generateArray(schema, fieldGenerator)
      case Type.MAP    => Left(new NotImplementedException("MAP type not supported"))
      case Type.FIXED  => Left(new NotImplementedException("FIXED type not supported"))
      // primitive
      case Type.ENUM | Type.STRING | Type.BYTES | Type.INT | Type.LONG | Type.FLOAT | Type.DOUBLE | Type.BOOLEAN =>
        fieldGenerator.generate(schema)
      // just null
      case Type.NULL => fieldGenerator.generate(schema)
    }
  }
}

object AvroGenerator {
  def apply(fieldGenerator: AvroFieldGenerator): AvroGenerator = new AvroGenerator(fieldGenerator)
}
