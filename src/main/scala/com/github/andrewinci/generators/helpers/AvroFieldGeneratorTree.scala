package com.github.andrewinci.generators.helpers

import com.github.andrewinci.core.AvroFieldGenerator
import com.github.andrewinci.core.FieldGeneratorException
import com.github.andrewinci.generators.EmptyAvroFieldGen
import org.apache.avro.Schema

case class AvroFieldGeneratorLeaf(gen: (Schema) => Either[FieldGeneratorException, Any]) extends AvroFieldGenerator {

  override def getGenerator(fieldName: String): Option[AvroFieldGenerator] =
    EmptyAvroFieldGen.getGenerator(fieldName)

  override def generate(schema: Schema): Either[FieldGeneratorException, Any] = gen(schema)
}

case class AvroFieldGeneratorNode(name: String, gen: AvroFieldGenerator) extends AvroFieldGenerator {

  override def getGenerator(fieldName: String): Option[AvroFieldGenerator] =
    if (fieldName == name) Some(gen) else None

  override def generate(schema: Schema): Either[FieldGeneratorException, Any] =
    EmptyAvroFieldGen.generate(schema)
}
