package com.github.andrewinci.generators

import com.github.andrewinci.impl.AvroGen
import munit.FunSuite
import org.apache.avro.Schema

import java.nio.ByteBuffer

class ConstAvroFieldGeneratorTest extends FunSuite {
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
    val fieldGenerator = new ConstAvroFieldGenerator(constDecimal = 12.23)

    val sut = AvroGen(fieldGenerator)
    // act
    val record = sut.generateRecord(schema)
    // assert
    assert(record.isRight)
    assertNotEquals(record.right.get.get("original").asInstanceOf[ByteBuffer].array().length, 0)
  }
}
