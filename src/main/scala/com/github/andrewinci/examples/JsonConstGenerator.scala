package com.github.andrewinci.examples

import com.github.andrewinci.AvroGenerator
import com.github.andrewinci.generators.AvroFieldGenerators
import com.github.andrewinci.generators.helpers.Compose.compose
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

object JsonConstGenerator extends App {

  val values =
    """
      |{
      |  "nestedRecord": {
      |    "nested2Record": {
      |      "testDoubleNested2": 1.23
      |    }
      |  }
      |}
      |""".stripMargin

  val avroSchema =
    """
      |{
      |  "type": "record",
      |  "name": "TestSchemaNested",
      |  "fields": [
      |    { "name": "testString", "type": "string"},
      |    { "name": "nestedRecord", "type": {"type": "record", "name": "TestSchemaNested2", "fields": [
      |        { "name": "testBooleanNested", "type": "boolean"},
      |        { "name": "nested2Record", "type": {"type": "record", "name": "TestSchemaNested3", "fields": [
      |          { "name": "testDoubleNested2", "type": "double"}
      |        ]}}
      |      ]}}
      |  ]
      |}
      |""".stripMargin

  val schema = new Schema.Parser().parse(avroSchema)

  /// Compose a json generator with a constant generator
  /// to use predefined values for undefined fields in the Json
  val result: Either[Throwable, GenericRecord] = AvroFieldGenerators
    .fromJson(values)
    .map(compose(_, AvroFieldGenerators.constantFieldsGen()))
    .map(AvroGenerator(_))
    .flatMap(_.generateRecord(schema))

  /// {"testString": "", "nestedRecord": {"testBooleanNested": true, "nested2Record": {"testDoubleNested2": 1.23}}}
}
