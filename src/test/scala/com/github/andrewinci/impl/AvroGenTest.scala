package com.github.andrewinci.impl

import com.github.andrewinci.core.FieldGenerator
import munit.FunSuite
import org.apache.avro.Schema

class AvroGenTest extends FunSuite {

  test("Gen happy path") {
    // arrange
    val sampleSchema = """{"type": "record", "name": "myrec","fields": [{ "name": "original", "type": "string" }]}"""
    val schema: Schema = new Schema.Parser().parse(sampleSchema)
    val fieldGenerator = new ConstFieldGen(str = "hello1")
    val sut = AvroGen(fieldGenerator)
    // act
    val record = sut.generateRandomAvro(schema)
    // assert
    assert(record.isRight)
    assertEquals(record.right.get.toString, """{"original": "hello1"}""")
  }

  test("Gen nested record happy path") {
    // arrange
    val sampleSchema =
      """{"type": "record", "name": "myrec","fields": [
        |{ "name": "original", "type": {"type": "record", "name": "myrecnested","fields": [
        |{ "name": "nested", "type": "string" }]}
        |}]}""".stripMargin
    val schema: Schema = new Schema.Parser().parse(sampleSchema)
    val fieldGenerator = new ConstFieldGen(str = "hello")

    val sut = AvroGen(fieldGenerator)
    // act
    val record = sut.generateRandomAvro(schema)
    // assert
    assert(record.isRight)
    assertEquals(record.right.get.toString, """{"original": {"nested": "hello"}}""")
  }

  test("Gen happy path  - enum") {
    // arrange
    val sampleSchema =
      """{"type": "record", "name": "myrec","fields": [
        |{ "name": "original", "type": {
        |  "type": "enum",
        |  "name": "Suit",
        |  "symbols" : ["SPADES", "HEARTS", "DIAMONDS", "CLUBS"]
        |}}
        |]}""".stripMargin
    val schema: Schema = new Schema.Parser().parse(sampleSchema)
    val fieldGenerator = new ConstFieldGen(str = "hello1")
    val sut = AvroGen(fieldGenerator)
    // act
    val record = sut.generateRandomAvro(schema)
    // assert
    assert(record.isRight, record)
    assertEquals(record.right.get.toString, """{"original": "SPADES"}""")
  }
}
