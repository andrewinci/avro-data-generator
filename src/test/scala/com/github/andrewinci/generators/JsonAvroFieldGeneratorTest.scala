package com.github.andrewinci.generators

import com.github.andrewinci.AvroGenerator
import munit.FunSuite
import org.apache.avro.Schema

class JsonAvroFieldGeneratorTest extends FunSuite {

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
    val gen = AvroFieldGenerator
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
    val res = gen
      .map(AvroGenerator(_))
      .flatMap(_.generateRecord(schema))

    assertEquals(
      res.map(_.toString),
      Right(
        """{"testNull": null, "testBoolean": false, "testInt": 123, "testLong": 321321321, "testFloat": 123.321, "testDouble": 32.12321, "testString": "the brown fox jumps over the lazy dog"}"""
      )
    )
  }

  test("Happy path - fromJson - bytes") {
    val sampleSchema =
      """{"type": "record", "name": "myrec","fields": [
        |{ "name": "testBytes", "type": "bytes"}
        |]}""".stripMargin
    val schema: Schema = new Schema.Parser().parse(sampleSchema)

    // act
    val gen = AvroFieldGenerator.fromJson("""{"testBytes": YW55IGNhcm5hbCBwbGVhcw== }""")

    // assert
    val res = gen
      .map(AvroGenerator(_))
      .flatMap(_.generateRecord(schema))

    assertEquals(
      res.map(_.toString),
      Right(
        """{"testBytes": "0x00"}"""
      )
    )
  }

  test("Happy path - fromJson - enum") {
    val sampleSchema =
      """{"type": "record", "name": "myrec","fields": [
        |{ "name": "test", "type": {
        |  "type": "enum",
        |  "name": "Suit",
        |  "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
        |}}
        |]}""".stripMargin
    val schema: Schema = new Schema.Parser().parse(sampleSchema)

    // act
    val gen = AvroFieldGenerator.fromJson("""{"test": "CLUBS" }""")

    // assert
    val res = gen
      .map(AvroGenerator(_))
      .flatMap(_.generateRecord(schema))

    assertEquals(
      res.map(_.toString),
      Right(
        """{"test": "CLUBS"}"""
      )
    )
  }

  test("fromJson - invalid enum") {
    val sampleSchema =
      """{"type": "record", "name": "myrec","fields": [
        |{ "name": "test", "type": {
        |  "type": "enum",
        |  "name": "Suit",
        |  "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
        |}}
        |]}""".stripMargin
    val schema: Schema = new Schema.Parser().parse(sampleSchema)

    // act
    val gen = AvroFieldGenerator.fromJson("""{"test": "TEST" }""")

    // assert
    val res = gen
      .map(AvroGenerator(_))
      .flatMap(_.generateRecord(schema))

    assert(res.isLeft)
  }
}
