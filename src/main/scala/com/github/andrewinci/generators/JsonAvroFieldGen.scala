package com.github.andrewinci.generators

import com.fasterxml.jackson.databind.JsonNode
import com.github.andrewinci.core.AvroFieldGenerator
import com.github.andrewinci.core.FieldGeneratorException
import com.github.andrewinci.generators.helpers.AvroFieldGeneratorHelper.avroEnumPicker
import com.github.andrewinci.generators.helpers.AvroFieldGeneratorLeaf
import org.apache.avro.Schema
import org.apache.avro.Schema.Type

class JsonAvroFieldGen(json: JsonNode) extends AvroFieldGenerator {

  def unsupportedField(field: String) = Left(new FieldGeneratorException(s"Field $field not supported"))
  def invalidField(field: String) = Left(new FieldGeneratorException(s"Invalid $field type"))

  override def getGenerator(fieldName: String): Option[AvroFieldGenerator] = {
    Option(if (json.isArray) json.get(fieldName.toInt) else json.get(fieldName))
      .map(field => {
        if (field.isObject || field.isArray) JsonAvroFieldGen(field)
        else
          AvroFieldGeneratorLeaf(schema =>
            schema.getType match {
              case Type.INT     => Right(field.numberValue().intValue())
              case Type.LONG    => Right(field.numberValue().longValue())
              case Type.DOUBLE  => Right(field.numberValue().doubleValue())
              case Type.FLOAT   => Right(field.numberValue().floatValue())
              case Type.STRING  => Right(field.textValue())
              case Type.BOOLEAN => Right(field.booleanValue())
              case Type.NULL    => Right(null)
              case Type.ENUM    => avroEnumPicker(schema)(_.find(field.textValue() == _))
              case Type.BYTES   => unsupportedField(field.toString)
              case Type.MAP     => unsupportedField(field.toString)
              case Type.FIXED   => unsupportedField(field.toString)
              case Type.RECORD  => invalidField(field.toString)
              case Type.ARRAY   => invalidField(field.toString)
              case Type.UNION   => invalidField(field.toString)
              case _            => invalidField(field.toString)
            }
          )
      })
  }

  override def generate(schema: Schema): Either[FieldGeneratorException, Any] =
    EmptyAvroFieldGen.generate(schema)
}

object JsonAvroFieldGen {
  def apply(jsonNode: JsonNode) = new JsonAvroFieldGen(jsonNode)
}