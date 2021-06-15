package com.github.andrewinci.core

import org.apache.avro.Schema

trait FieldGenerator {

  def getGenerator(fieldName: String): Option[FieldGenerator]

  def generate(schema: Schema): Object
}
