package com.github.andrewinci.generators

import com.github.andrewinci.AvroGenerator
import munit.FunSuite
import org.apache.avro.Schema

class JsonAvroFieldGenTest extends FunSuite {

  test("Happy path - fromJson - primitive types") {
    val sampleSchema =
      """{"type": "record", "name": "myrec","fields": [
        |{ "name": "testNull", "type": "null"},
        |{ "name": "testBoolean", "type": "boolean"},
        |{ "name": "testInt", "type": "int"},
        |{ "name": "testLong", "type": "long"},
        |{ "name": "testFloat", "type": "float"},
        |{ "name": "testDouble", "type": "double"},
        |{ "name": "testString", "type": "string"}
        |]}""".stripMargin
    val schema: Schema = new Schema.Parser().parse(sampleSchema)

    // act
    val gen = AvroFieldGen
      .fromJson("""
                  |{
                  |"testNull": null,
                  |"testBoolean": false,
                  |"testInt": 123,
                  |"testLong": 321321321,
                  |"testFloat": 123.321,
                  |"testDouble": 32.12321,
                  |"testString": "the brown fox jumps over the lazy dog"
                  |}
                  |""".stripMargin)

    // assert
    val res = gen.map(AvroGenerator(_)).flatMap(_.generateRecord(schema))

    assertEquals(
      res.map(_.toString),
      Right(
        """{"testNull": null, "testBoolean": false, "testInt": 123, "testLong": 321321321, "testFloat": 123.321, "testDouble": 32.12321, "testString": "the brown fox jumps over the lazy dog"}"""
      )
    )
  }

  test("Happy path - fromJson - enum") {
    val sampleSchema =
      """{"type": "record", "name": "myrec", "fields": [
        |{ "name": "test", "type": {
        |  "type": "enum",
        |  "name": "Suit",
        |  "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
        |}}
        |]}""".stripMargin
    val schema: Schema = new Schema.Parser().parse(sampleSchema)

    // act
    val gen = AvroFieldGen.fromJson("""{"test": "CLUBS" }""")

    // assert
    val res = gen.map(AvroGenerator(_)).flatMap(_.generateRecord(schema))

    assertEquals(res.map(_.toString), Right("""{"test": "CLUBS"}"""))
  }

  test("fromJson - invalid enum") {
    val sampleSchema =
      """{"type": "record", "name": "myrec", "fields": [
        |{ "name": "test", "type": {
        |  "type": "enum",
        |  "name": "Suit",
        |  "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
        |}}
        |]}""".stripMargin
    val schema: Schema = new Schema.Parser().parse(sampleSchema)

    // act
    val gen = AvroFieldGen.fromJson("""{"test": "TEST" }""")

    // assert
    val res = gen.map(AvroGenerator(_)).flatMap(_.generateRecord(schema))

    assert(res.isLeft)
  }

  test("Happy path - fromJson - array") {
    val sampleSchema =
      """{"type": "record", "name": "myrec", "fields": [
        |{ "name": "test", "type": {"type": "array", "items": "string"}}
        |]}""".stripMargin
    val schema: Schema = new Schema.Parser().parse(sampleSchema)

    // act
    val gen = AvroFieldGen.fromJson("""{"test": ["A", "B", "C"]}""")

    // assert
    val res = gen.map(AvroGenerator(_)).flatMap(_.generateRecord(schema))

    assertEquals(res.map(_.toString), Right("""{"test": ["A", "B", "C"]}"""))
  }

  test("fromJson - empty array") {
    val sampleSchema =
      """{"type": "record", "name": "myrec", "fields": [
        |{ "name": "test", "type": {"type": "array", "items": "string"}}
        |]}""".stripMargin
    val schema: Schema = new Schema.Parser().parse(sampleSchema)

    // act
    val gen = AvroFieldGen.fromJson("""{"test": []}""")

    // assert
    val res = gen.map(AvroGenerator(_)).flatMap(_.generateRecord(schema))

    assertEquals(res.map(_.toString), Right("""{"test": []}"""))
  }

  test("fromJson - array of records") {
    val sampleSchema =
      """{"type": "record", "name": "myrec", "fields": [
        |{ "name": "test", "type": {"type": "array", "items": 
        |   {"type": "record", "name": "myrec2","fields": [
        |        { "name": "testString", "type": "string"},
        |        { "name": "testBoolean", "type": "boolean"}
        |        ]}
        |}}
        |]}""".stripMargin
    val schema: Schema = new Schema.Parser().parse(sampleSchema)

    // act
    val gen = AvroFieldGen.fromJson("""{"test": [{"testString": "123", "testBoolean": true}]}""")

    // assert
    val res = gen.map(AvroGenerator(_)).flatMap(_.generateRecord(schema))

    assertEquals(res.map(_.toString), Right("""{"test": [{"testString": "123", "testBoolean": true}]}"""))
  }
}
