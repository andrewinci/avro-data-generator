package com.github.andrewinci.helper

import com.github.andrewinci.AvroGenerator
import com.github.andrewinci.generators.AvroFieldGen
import munit.FunSuite
import org.apache.avro.Schema

import scala.io.Source

class AvroToJsonTest extends FunSuite {
  test("Test parse Generic record to json") {
    // arrange
    def normalizeJson(json: String): String = json.replace("\n", "").replace(" ", "")
    val json = Source.fromResource("json/testSchemaNested.json").mkString
    val schema = new Schema.Parser().parse(Source.fromResource("testSchemaNested.avsc").mkString)
    val fieldGenerator = AvroFieldGen.fromJson(json).right.get
    val generator = AvroGenerator(fieldGenerator)
    // act
    val res = generator.generateRecord(schema)

    // assert
    assert(res.isRight, s"Gen output should be right instead ${res.left.get}")
    //todo: enable
    assertEquals(res.map(AvroToJson.parseRecord).right.get.toString, normalizeJson(json), "Invalid content generated")
  }
}
