package com.github.andrewinci.generators

import com.github.andrewinci.AvroGenerator
import munit.FunSuite
import org.apache.avro.Schema

import java.nio.ByteBuffer

class ConstAvroFieldGenTest extends FunSuite {
  test("Gen nested record happy path") {
    // arrange
    val sampleSchema =
      """{"type": "record", "name": "myrec","fields": [
        { "name": "original", "type": {
              "type": "bytes",
              "logicalType": "decimal",
              "precision": 4,
              "scale": 2
            }
        }]}"""
    val schema: Schema = new Schema.Parser().parse(sampleSchema)
    val fieldGenerator = new ConstAvroFieldGen(constDecimal = 12.23)

    val sut = AvroGenerator(fieldGenerator)
    // act
    val record = sut.generateRecord(schema)
    // assert
    assert(record.isRight)
    assertNotEquals(record.right.get.get("original").asInstanceOf[ByteBuffer].array().length, 0)
  }

  test("Constant generator - generate longs") {
    // arrange
    val sampleSchema =
      """{"type": "record", "name": "myrec","fields": [
        { "name": "original", "type": "long"}
        ]}"""
    val schema: Schema = new Schema.Parser().parse(sampleSchema)
    val fieldGenerator = new ConstAvroFieldGen(constInt = 123)

    val sut = AvroGenerator(fieldGenerator)
    // act
    val record = sut.generateRecord(schema)
    // assert
    assert(record.isRight)
    assertEquals(record.right.get.toString, """{"original": 123}""")
  }
}
