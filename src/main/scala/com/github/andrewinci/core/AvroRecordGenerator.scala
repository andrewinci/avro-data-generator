package com.github.andrewinci.core

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

trait AvroRecordGenerator {

  /** Generate an avro record
    *
    * @param schema the avro schema to use to generate the record
    * @return a random record with the provided schema
    */
  def generateRecord(schema: Schema): Either[AvroGeneratorException, GenericRecord]
}
