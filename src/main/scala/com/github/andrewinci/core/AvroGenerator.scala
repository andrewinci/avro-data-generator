package com.github.andrewinci.core

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

trait AvroGenerator {
  /**
   * Generate a random avro record
   *
   * @param schema the avro schema to use to generate the record
   * @return a random record with the provided schema
   */
  def generateRandomAvro(schema: Schema): Either[AvroGeneratorException, GenericRecord]

  //
  //  /**
  //   * Generate the minimal avro record
  //   * setting to null any optional argument
  //   * and picking the record with less fields
  //   * in unions
  //   *
  //   * @param schema the avro schema to use to generate the record
  //   * @return a random record with the provided schema
  //   */
  //  def generateMinimalRandomAvro(schema: Schema): GenericRecord
}
