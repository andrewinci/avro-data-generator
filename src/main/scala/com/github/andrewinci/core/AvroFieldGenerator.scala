package com.github.andrewinci.core

import org.apache.avro.Schema

trait AvroFieldGenerator {

  def getGenerator(fieldName: String): Option[AvroFieldGenerator]

  def generate(schema: Schema): Either[FieldGeneratorException, Any]
}
