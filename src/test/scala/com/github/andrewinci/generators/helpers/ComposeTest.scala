package com.github.andrewinci.generators.helpers

import com.github.andrewinci.generators.AvroFieldGenerator
import com.github.andrewinci.generators.ConstAvroFieldGenerator
import com.github.andrewinci.generators.helpers.Compose.compose
import com.github.andrewinci.impl.AvroGen

import munit.FunSuite
import org.apache.avro.Schema

class ComposeTest extends FunSuite {
  test("Compose happy path") {
    // arrange
    val sampleSchema =
      """{"type": "record", "name": "myrec","fields": [
        |{ "name": "f1", "type": "string" },
        |{ "name": "f2", "type": "string" }
        |]}""".stripMargin
    val schema: Schema = new Schema.Parser().parse(sampleSchema)
    val fieldGenerator = compose(
      AvroFieldGenerator.fromMap("f1" -> (_ => Right("test"))),
      new ConstAvroFieldGenerator(constStr = "default")
    )
    val sut = AvroGen(fieldGenerator)
    // act
    val record = sut.generateRecord(schema)
    // assert
    assert(record.isRight)
    assertEquals(record.right.get.toString, """{"f1": "test", "f2": "default"}""")
  }

  test("Compose happy path - nested") {
    // arrange
    val sampleSchema =
      """{"type": "record", "name": "myrec","fields": [
        |{ "name": "original", "type": {"type": "record", "name": "myrecnested","fields": [
        |{ "name": "nested1", "type": "string" },
        |{ "name": "nested2", "type": "int" }
        |]}
        |}]}""".stripMargin
    val schema: Schema = new Schema.Parser().parse(sampleSchema)
    val fieldGenerator = compose(
      AvroFieldGenerator.fromMap("original.nested1" -> (_ => Right("test"))),
      new ConstAvroFieldGenerator(constInt = 123)
    )
    val sut = AvroGen(fieldGenerator)
    // act
    val record = sut.generateRecord(schema)
    // assert
    assert(record.isRight)
    assertEquals(record.right.get.toString, """{"original": {"nested1": "test", "nested2": 123}}""")
  }

  test("First generator has precedence in composition") {
    // arrange
    val sampleSchema =
      """{"type": "record", "name": "myrec","fields": [
        |{ "name": "original", "type": ["string", "boolean"] }
        |]}""".stripMargin
    val schema: Schema = new Schema.Parser().parse(sampleSchema)
    val fieldGenerator = compose(
      AvroFieldGenerator.fromMap("original" -> (_ => Right(false))),
      new ConstAvroFieldGenerator(constStr = "str")
    )

    val sut = AvroGen(fieldGenerator)
    // act
    val record = sut.generateRecord(schema)
    // assert
    assert(record.isRight)
    assertEquals(record.right.get.toString, """{"original": false}""")
  }

  test("First generator has precedence in composition 2") {
    // arrange
    val sampleSchema =
      """{"type": "record", "name": "myrec","fields": [
        |{ "name": "original", "type": ["string", "boolean"] }
        |]}""".stripMargin
    val schema: Schema = new Schema.Parser().parse(sampleSchema)
    val fieldGenerator = compose(
      new ConstAvroFieldGenerator(constStr = "str"),
      AvroFieldGenerator.fromMap("original" -> (_ => Right(false)))
    )

    val sut = AvroGen(fieldGenerator)
    // act
    val record = sut.generateRecord(schema)
    // assert
    assert(record.isRight)
    assertEquals(record.right.get.toString, """{"original": "str"}""")
  }
}
