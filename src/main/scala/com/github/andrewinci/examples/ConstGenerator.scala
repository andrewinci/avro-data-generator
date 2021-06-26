package com.github.andrewinci.examples

import com.github.andrewinci.AvroGenerator
import com.github.andrewinci.generators.AvroFieldGenerators
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

object ConstGenerator extends App {

  val avroSchema =
    """
      |{
      |  "type": "record",
      |  "name": "TestSchemaFields",
      |  "fields": [
      |    { "name": "testNull", "type": "null"},
      |    { "name": "testBoolean", "type": "boolean"},
      |    { "name": "testInt", "type": "int"},
      |    { "name": "testLong", "type": "long"},
      |    { "name": "testFloat", "type": "float"},
      |    { "name": "testDouble", "type": "double"},
      |    { "name": "testString", "type": "string"},
      |    { "name": "testBytes", "type": "bytes"},
      |    { "name": "decimal", "type": {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}},
      |    { "name": "uuid", "type": {"type": "string", "logicalType": "uuid"}},
      |    { "name": "date", "type": {"type": "int", "logicalType": "date"}},
      |    { "name": "timeMillis", "type": {"type": "int", "logicalType": "time-millis"}},
      |    { "name": "timeMicros", "type": {"type": "long", "logicalType": "time-micros"}},
      |    { "name": "timestampMillis", "type": {"type": "long", "logicalType": "timestamp-millis"}},
      |    { "name": "timestampMicros", "type": {"type": "long", "logicalType": "timestamp-micros"}},
      |    { "name": "localTimestampMillis", "type": {"type": "long", "logicalType": "local-timestamp-millis"}},
      |    { "name": "localTimestampMicros", "type": {"type": "long", "logicalType": "local-timestamp-micros"}}
      |  ]
      |}
      |""".stripMargin

  val schema = new Schema.Parser().parse(avroSchema)

  val result = AvroGenerator(
    AvroFieldGenerators.constantFieldsGen(str = "Constant value for every string")
  ).generateRecord(schema)

  /// {"testNull": null, "testBoolean": true, "testInt": 0, "testLong": 0, "testFloat": 0.1, "testDouble": 0.1, "testString": "Constant value for every string", "testBytes": "\u0001\u0002\u0003\u0004", "decimal": "\n", "uuid": "579cc0f0-efa7-4086-b75c-7848fb7de6a8", "date": 18804, "timeMillis": 27826579, "timeMicros": 27826579616, "timestampMillis": 1624689826579733, "timestampMicros": 1624689826579733, "localTimestampMillis": 1624693426579680, "localTimestampMicros": 1624693426579680}
}
