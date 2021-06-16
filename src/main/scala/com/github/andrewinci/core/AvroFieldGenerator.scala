package com.github.andrewinci.core

import org.apache.avro.Schema

trait AvroFieldGenerator {

  def getGenerator(fieldName: String): Option[AvroFieldGenerator]

  def generate(schema: Schema): Either[FieldGeneratorException, Any]
}

case class AvroFieldGeneratorLeaf(gen: (Schema) => Either[FieldGeneratorException, Any]) extends AvroFieldGenerator {
  override def getGenerator(fieldName: String): Option[AvroFieldGenerator] = None
  override def generate(schema: Schema): Either[FieldGeneratorException, Any] = gen(schema)
}

case class AvroFieldGeneratorNode(name: String, gen: AvroFieldGenerator) extends AvroFieldGenerator {

  override def getGenerator(fieldName: String): Option[AvroFieldGenerator] =
    if (fieldName == name) Some(gen) else None

  override def generate(schema: Schema): Either[FieldGeneratorException, Any] =
    Left(new FieldGeneratorException(s"Unable to set $name with an instance of ${schema.getType.getName}"))
}
