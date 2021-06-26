package com.github.andrewinci.examples

import com.github.andrewinci.AvroGenerator
import com.github.andrewinci.generators.AvroFieldGenerators
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

object JsonGenerator extends App {

  val values =
    """
      |{
      |  "testNull": null,
      |  "testBoolean": true,
      |  "testInt": 1,
      |  "testLong": 23123123,
      |  "testFloat": 1.2,
      |  "testDouble": 1.23,
      |  "testString": "123123",
      |  "testBytes": "ZWFzdXJlLg==",
      |  "decimal": 12.32,
      |  "uuid": "123e4567-e89b-12d3-a456-426614174000",
      |  "date": "2007-12-03",
      |  "timeMillis": "10:15:30",
      |  "timeMicros": "10:15:30",
      |  "timestampMillis": "2007-12-03T10:15:30Z",
      |  "timestampMicros": "2007-12-03T10:15:30Z",
      |  "localTimestampMillis": "2007-12-03T10:15:30",
      |  "localTimestampMicros": "2007-12-03T10:15:30"
      |}
      |""".stripMargin

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

  val result: Either[Throwable, GenericRecord] = AvroFieldGenerators
    .fromJson(values)
    .map(AvroGenerator(_))
    .flatMap(_.generateRecord(schema))

  /// {"testNull": null, "testBoolean": true, "testInt": 1, "testLong": 23123123, "testFloat": 1.2, "testDouble": 1.23, "testString": "123123", "testBytes": "easure.", "decimal": "\u0004Ð", "uuid": "123e4567-e89b-12d3-a456-426614174000", "date": 13850, "timeMillis": 36930000, "timeMicros": 36930000000, "timestampMillis": 1196676930000000, "timestampMicros": 1196676930000000, "localTimestampMillis": 1196676930000000, "localTimestampMicros": 1196676930000000}
}
