package com.github.andrewinci.generators

import com.github.andrewinci.core.AvroFieldGenerator
import com.github.andrewinci.core.FieldGeneratorException
import org.apache.avro.Schema

object EmptyAvroFieldGenerator extends AvroFieldGenerator {
  override def getGenerator(fieldName: String): Option[AvroFieldGenerator] = None

  override def generate(schema: Schema): Either[FieldGeneratorException, Any] =
    Left(new FieldGeneratorException(s"Unable to generate an instance of ${schema.getType.getName}"))
}
