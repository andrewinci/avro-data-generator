package com.github.andrewinci.impl

import com.github.andrewinci.core.{AvroGenerator, AvroGeneratorException, FieldGenerator, NotImplementedException}
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericData

class AvroGen(val fieldGenerator: FieldGenerator) extends AvroGenerator {

  /**
   * Generate a random avro record
   *
   * @param schema the avro schema to use to generate the record
   * @return a random record with the provided schema
   */
  override def generateRandomAvro(schema: Schema): Either[AvroGeneratorException, GenericRecord] =
    if (schema.getType != Type.RECORD)
      Left(new AvroGeneratorException("Only RECORD is supported"))
    else generateRecord(schema, fieldGenerator)

  def generateRecord(schema: Schema, fieldGenerator: FieldGenerator): Either[AvroGeneratorException, GenericRecord] = {
    val record = new GenericData.Record(schema)
    schema.getFields.forEach(field => {
      fieldGenerator.getGenerator(field.name()) match {
        case Some(gen) => generateValue(field.schema(), gen) match {
          case Right(value) => record.put(field.name(), value)
          case Left(e) => return Left(new AvroGeneratorException(s"Unable to set value for ${field.name()}", e))
        }
        case None => return Left(new AvroGeneratorException(s"No generator specified for field ${field.name()}"))
      }
    })
    if (new GenericData().validate(schema, record))
      Right(record)
    else Left(new AvroGeneratorException(s"Invalid avro generated"))
  }

  def generateValue(schema: Schema, fieldGenerator: FieldGenerator): Either[AvroGeneratorException, Object] = {
    schema.getType match {
      // complex data types
      case Type.RECORD => generateRecord(schema, fieldGenerator)
      case Type.UNION => Left(new NotImplementedException("UNION type not supported"))
      case Type.MAP => Left(new NotImplementedException("MAP type not supported"))
      case Type.ARRAY => Left(new NotImplementedException("ARRAY type not supported"))
      case Type.FIXED => Left(new NotImplementedException("FIXED type not supported"))
      // primitive
      case Type.ENUM => Left(new NotImplementedException("ENUM type not supported"))
      case Type.STRING | Type.BYTES | Type.INT | Type.LONG | Type.FLOAT | Type.DOUBLE | Type.BOOLEAN
      => Right(fieldGenerator.generate(schema))
      // just null
      case Type.NULL => null
    }
  }
}

object AvroGen {
  def apply(fieldGenerator: FieldGenerator): AvroGen = new AvroGen(fieldGenerator)
}