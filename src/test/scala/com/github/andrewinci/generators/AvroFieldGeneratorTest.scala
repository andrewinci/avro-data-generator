package com.github.andrewinci.generators

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

  test("Happy path - fromMap - shared keys") {
    // arrange

    // act
    val gen = AvroFieldGenerator.fromMap(
      "a1.a2.a3" -> (_ => Right("A")),
      "a1.a2.b3" -> (_ => Right("B")),
      "a2.c2.a3" -> (_ => Right("C")),
      "a2.c2.b3" -> (_ => Right("D"))
    )

    // assert
    val leafA = gen.getGenerator("a1").flatMap(_.getGenerator("a2")).flatMap(_.getGenerator("a3"))
    val leafB = gen.getGenerator("a1").flatMap(_.getGenerator("a2")).flatMap(_.getGenerator("b3"))
    val leafC = gen.getGenerator("a2").flatMap(_.getGenerator("c2")).flatMap(_.getGenerator("a3"))
    val leafD = gen.getGenerator("a2").flatMap(_.getGenerator("c2")).flatMap(_.getGenerator("b3"))

    assert(leafA.nonEmpty)
    assert(leafB.nonEmpty)
    assert(leafC.nonEmpty)
    assert(leafD.nonEmpty)
    assertEquals(leafA.get.generate(null), Right("A"))
    assertEquals(leafB.get.generate(null), Right("B"))
    assertEquals(leafC.get.generate(null), Right("C"))
    assertEquals(leafD.get.generate(null), Right("D"))
  }
}
