package com.github.andrewinci.impl

import com.github.andrewinci.core.AvroFieldGenerator
import com.github.andrewinci.generators.AvroFieldGenerator
import com.github.andrewinci.generators.ConstAvroFieldGenerator
import com.github.andrewinci.generators.helpers.AvroFieldGeneratorLeaf
import com.github.andrewinci.generators.helpers.AvroFieldGeneratorNode
import com.github.andrewinci.generators.helpers.Compose.compose
import munit.FunSuite
import org.apache.avro.Schema

class AvroGenTest extends FunSuite {

  test("Gen happy path") {
    // arrange
    val sampleSchema = """{"type": "record", "name": "myrec","fields": [{ "name": "original", "type": "string" }]}"""
    val schema: Schema = new Schema.Parser().parse(sampleSchema)
    val fieldGenerator = new ConstAvroFieldGenerator(constStr = "hello1")
    val sut = AvroGen(fieldGenerator)
    // act
    val record = sut.generateRecord(schema)
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
    val fieldGenerator = new ConstAvroFieldGenerator(constStr = "hello")

    val sut = AvroGen(fieldGenerator)
    // act
    val record = sut.generateRecord(schema)
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
    val fieldGenerator = new ConstAvroFieldGenerator(constStr = "hello1")
    val sut = AvroGen(fieldGenerator)
    // act
    val record = sut.generateRecord(schema)
    // assert
    assert(record.isRight, record)
    assertEquals(record.right.get.toString, """{"original": "SPADES"}""")
  }

  test("Gen happy path - union") {
    // arrange
    val sampleSchema =
      """{"type": "record", "name": "myrec","fields": [
        |{ "name": "original", "type": ["string", "boolean"] }
        |]}""".stripMargin
    val schema: Schema = new Schema.Parser().parse(sampleSchema)
    val fieldGenerator = AvroFieldGenerator.fromMap("original" -> (_ => Right(false)))

    val sut = AvroGen(fieldGenerator)
    // act
    val record = sut.generateRecord(schema)
    // assert
    assert(record.isRight)
    assertEquals(record.right.get.toString, """{"original": false}""")
  }

  test("Gen - union - left if no object in the union is buildable") {
    // arrange
    val sampleSchema =
      """{"type": "record", "name": "myrec","fields": [
        |{ "name": "original", "type": ["string", "boolean"] }
        |]}""".stripMargin
    val schema: Schema = new Schema.Parser().parse(sampleSchema)
    val fieldGenerator = AvroFieldGenerator.fromMap("original" -> (_ => Right(1.6)))

    val sut = AvroGen(fieldGenerator)
    // act
    val record = sut.generateRecord(schema)
    // assert
    assert(record.isLeft)
  }

  test("Gen return left if no generator is available for a specific field") {
    // arrange
    val sampleSchema =
      """{"type": "record", "name": "myrec","fields": [
        |{ "name": "original", "type": ["string", "boolean"] }
        |]}""".stripMargin
    val schema: Schema = new Schema.Parser().parse(sampleSchema)
    val fieldGenerator = AvroFieldGenerator.fromMap("unlistedField" -> (_ => Right(1.6)))

    val sut = AvroGen(fieldGenerator)
    // act
    val record = sut.generateRecord(schema)
    // assert
    assert(record.isLeft)
  }

  test("Gen - union - respect the type") {
    // arrange
    val sampleSchema =
      """{"type": "record", "name": "myrec","fields": [
        |{ "name": "original", "type": ["string", "int"] }
        |]}""".stripMargin
    val schema: Schema = new Schema.Parser().parse(sampleSchema)
    val fieldGenerator = AvroFieldGenerator.fromMap("original" -> (_ => Right("1231")))

    val sut = AvroGen(fieldGenerator)
    // act
    val record = sut.generateRecord(schema)
    // assert
    assert(record.isRight)
    assertEquals(record.right.get.toString, """{"original": "1231"}""")
  }

  test("Gen - union - set null") {
    // arrange
    val sampleSchema =
      """{"type": "record", "name": "myrec","fields": [
        |{ "name": "original", "type": ["string", "null"] }
        |]}""".stripMargin
    val schema: Schema = new Schema.Parser().parse(sampleSchema)
    val fieldGenerator = AvroFieldGenerator.fromMap("original" -> (_ => Right(null)))

    val sut = AvroGen(fieldGenerator)
    // act
    val record = sut.generateRecord(schema)
    // assert
    assert(record.isRight)
    assertEquals(record.right.get.toString, """{"original": null}""")
  }

  test("Gen - array - happy path") {
    // arrange
    val sampleSchema =
      """{"type": "record", "name": "myrec","fields": [
        |{ "name": "original", "type": {
        |  "type": "array",
        |  "items" : "string"  
        | }}
        |]}""".stripMargin
    val schema: Schema = new Schema.Parser().parse(sampleSchema)
    val fieldGenerator = AvroFieldGenerator.fromMap("original.0" -> (_ => Right("test1")))

    val sut = AvroGen(fieldGenerator)
    // act
    val record = sut.generateRecord(schema)
    // assert
    assert(record.isRight)
    assertEquals(record.right.get.toString, """{"original": ["test1"]}""")
  }

  test("Gen - array - happy path - multiple elements") {
    // arrange
    val sampleSchema =
      """{"type": "record", "name": "myrec","fields": [
        |{ "name": "original", "type": {
        |  "type": "array",
        |  "items" : "string"  
        | }}
        |]}""".stripMargin
    val schema: Schema = new Schema.Parser().parse(sampleSchema)
    val fieldGenerator = AvroFieldGenerator.fromMap(
      "original.0" -> (_ => Right("test0")),
      "original.1" -> (_ => Right("test1")),
      "original.2" -> (_ => Right("test2"))
    )

    val sut = AvroGen(fieldGenerator)
    // act
    val record = sut.generateRecord(schema)
    // assert
    assert(record.isRight)
    assertEquals(record.right.get.toString, """{"original": ["test0", "test1", "test2"]}""")
  }
}
