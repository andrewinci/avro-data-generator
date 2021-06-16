package com.github.andrewinci.generators

import com.github.andrewinci.generators.AvroFieldGenerator.FieldNameToAvroGen
import munit.FunSuite
import org.apache.avro.Schema

class AvroFieldGeneratorTest extends FunSuite {

  test("Happy path - fromMap") {
    // arrange
    val sampleSchema = """{"type": "record", "name": "myrec","fields": [{ "name": "original", "type": "string" }]}"""
    val schema: Schema = new Schema.Parser().parse(sampleSchema)

    // act
    val gen = AvroFieldGenerator.fromMap(
      "a1.a2.a3" -> (_ => Right("a")),
      "b1.b2.b3" -> (_ => Right("b")),
      "c1.c2.c3" -> (_ => Right("c"))
    )

    // assert
    val leafA = gen.getGenerator("a1").flatMap(_.getGenerator("a2")).flatMap(_.getGenerator("a3"))
    val leafB = gen.getGenerator("b1").flatMap(_.getGenerator("b2")).flatMap(_.getGenerator("b3"))
    val leafC = gen.getGenerator("c1").flatMap(_.getGenerator("c2")).flatMap(_.getGenerator("c3"))

    assert(leafA.nonEmpty)
    assert(leafB.nonEmpty)
    assert(leafC.nonEmpty)
    assertEquals(leafA.get.generate(schema), Right("a"))
    assertEquals(leafB.get.generate(schema), Right("b"))
    assertEquals(leafC.get.generate(schema), Right("c"))
  }
}
