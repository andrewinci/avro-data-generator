package com.github.andrewinci

import com.github.andrewinci.generators.AvroFieldGen
import com.github.andrewinci.helper.AvroToJson
import munit.FunSuite
import org.apache.avro.Schema
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.JsonDecoder
import org.apache.avro.io.JsonEncoder

import java.io.ByteArrayOutputStream
import scala.io.Source

class JsonGeneratorIntegrationTest extends FunSuite {

  def testJsonGen(
      name: String,
      schemaPath: String,
      expectedResPath: String
  )(implicit loc: munit.Location): Unit = {
    test(name) {
      // arrange
      def normalizeJson(json: String): String = json.replace("\n", "").replace(" ", "")
      val json = Source.fromResource(expectedResPath).mkString
      val schema = new Schema.Parser().parse(Source.fromResource(schemaPath).mkString)
      val fieldGenerator = AvroFieldGen.fromJson(json).right.get
      val generator = AvroGenerator(fieldGenerator)
      // act
      val res = generator.generateRecord(schema)

      // assert
      assert(res.isRight, s"Gen output should be right instead ${res.left.get}")
      assertEquals(
        res.map(AvroToJson.parseRecord(_).toString).map(normalizeJson).right.get,
        normalizeJson(json),
        "Invalid content generated"
      )
    }
  }

  testJsonGen("Test json avro generator - nested", "testSchemaNested.avsc", "json/testSchemaNested.json")
  testJsonGen("Test json avro generator - fields", "testSchemaFields.avsc", "json/testSchemaField.json")
  testJsonGen("Test json avro generator - array", "testSchemaArray.avsc", "json/testSchemaArray.json")
  testJsonGen("Test json avro generator - union", "testSchemaUnion.avsc", "json/testSchemaUnion.json")
  testJsonGen("Test json avro generator - union2", "testSchemaUnion.avsc", "json/testSchemaUnion1.json")
  testJsonGen(
    "Test constant avro generator - logical type",
    "testSchemaLogicalTypes.avsc",
    "json/testSchemaLogicalTypes.json"
  )
}
