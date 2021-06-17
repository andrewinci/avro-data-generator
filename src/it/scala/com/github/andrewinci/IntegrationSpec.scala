package com.github.andrewinci

import com.github.andrewinci.generators.ConstAvroFieldGen
import munit.FunSuite
import org.apache.avro.Schema

import scala.io.Source

class IntegrationSpec extends FunSuite {
  test("Test constant avro generator") {
    // arrange
    val schema = new Schema.Parser().parse(Source.fromResource("testSchema.avsc").mkString)
    val fieldGenerator = new ConstAvroFieldGen()
    val generator = AvroGenerator(fieldGenerator)
    // act
    val res = generator.generateRecord(schema)
    // assert
    assert(res.isRight)
  }
}
