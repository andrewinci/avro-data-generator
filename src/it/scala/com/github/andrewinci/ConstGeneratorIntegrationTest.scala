package com.github.andrewinci

import com.github.andrewinci.generators.ConstAvroFieldGen
import munit.FunSuite
import org.apache.avro.Schema

import scala.io.Source

class ConstGeneratorIntegrationTest extends FunSuite {
  test("Test constant avro generator - nested") {
    // arrange
    val schema = new Schema.Parser().parse(Source.fromResource("testSchemaNested.avsc").mkString)
    val fieldGenerator = new ConstAvroFieldGen()
    val generator = AvroGenerator(fieldGenerator)
    // act
    val res = generator.generateRecord(schema)
    // assert
    assert(res.isRight)
  }

  test("Test constant avro generator - fields") {
    // arrange
    val schema = new Schema.Parser().parse(Source.fromResource("testSchemaFields.avsc").mkString)
    val fieldGenerator = new ConstAvroFieldGen()
    val generator = AvroGenerator(fieldGenerator)
    // act
    val res = generator.generateRecord(schema)
    // assert
    assert(res.isRight)
  }

  test("Test constant avro generator - array") {
    // arrange
    val schema = new Schema.Parser().parse(Source.fromResource("testSchemaArray.avsc").mkString)
    val fieldGenerator = new ConstAvroFieldGen()
    val generator = AvroGenerator(fieldGenerator)
    // act
    val res = generator.generateRecord(schema)
    // assert
    assert(res.isRight)
  }

  test("Test constant avro generator - union") {
    // arrange
    val schema = new Schema.Parser().parse(Source.fromResource("testSchemaUnion.avsc").mkString)
    val fieldGenerator = new ConstAvroFieldGen()
    val generator = AvroGenerator(fieldGenerator)
    // act
    val res = generator.generateRecord(schema)
    // assert
    assert(res.isRight)
  }

  test("Test constant avro generator - logical types") {
    // arrange
    val schema = new Schema.Parser().parse(Source.fromResource("testSchemaLogicalTypes.avsc").mkString)
    val fieldGenerator = new ConstAvroFieldGen()
    val generator = AvroGenerator(fieldGenerator)
    // act
    val res = generator.generateRecord(schema)
    // assert
    assert(res.isRight)
  }
}
