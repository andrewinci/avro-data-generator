package com.github.andrewinci.impl

import com.github.andrewinci.core.FieldGenerator
import munit.FunSuite
import org.apache.avro.Schema

class AvroGenTest extends FunSuite {

  test("Gen happy path") {
    // arrange
    val sampleSchema = """{"type": "record", "name": "myrec","fields": [{ "name": "original", "type": "string" }]}"""
    val schema: Schema = new Schema.Parser().parse(sampleSchema)
    var fieldGenerator: Option[FieldGenerator] = None
    fieldGenerator = Some(new FieldGenerator {
      override def getGenerator(fieldName: String): Option[FieldGenerator] = fieldGenerator

      override def generate(schema: Schema): AnyRef = "1"
    })

    val sut = AvroGen(fieldGenerator.get)
    // act
    val record = sut.generateRandomAvro(schema)
    // assert
    assert(record.isRight)
    assertEquals(record.right.get.toString, """{"original": "1"}""")
  }

  test("Gen nested record happy path") {
    // arrange
    val sampleSchema =
      """{"type": "record", "name": "myrec","fields": [
        |{ "name": "original", "type": {"type": "record", "name": "myrecnested","fields": [
        |{ "name": "nested", "type": "string" }]}
        |}]}""".stripMargin
    val schema: Schema = new Schema.Parser().parse(sampleSchema)
    var fieldGenerator: Option[FieldGenerator] = None
    fieldGenerator = Some(new FieldGenerator {
      override def getGenerator(fieldName: String): Option[FieldGenerator] = fieldGenerator

      override def generate(schema: Schema): AnyRef = "1"
    })

    val sut = AvroGen(fieldGenerator.get)
    // act
    val record = sut.generateRandomAvro(schema)
    // assert
    assert(record.isRight)
    assertEquals(record.right.get.toString, """{"original": {"nested": "1"}}""")
  }

}
