package com.github.andrewinci

import com.github.andrewinci.generators.AvroFieldGen
import munit.FunSuite
import org.apache.avro.Schema

import scala.io.Source

class ConstGeneratorIntegrationTest extends FunSuite {

  def testConstGen(
      name: String,
      schemaPath: String
  )(implicit loc: munit.Location): Unit = {
    test(name) {
      // arrange
      val schema = new Schema.Parser().parse(Source.fromResource(schemaPath).mkString)
      val fieldGenerator = AvroFieldGen.constantFieldsGen()
      val generator = AvroGenerator(fieldGenerator)
      // act
      val res = generator.generateRecord(schema)
      // assert
      assert(res.isRight)
    }
  }

  testConstGen("Test constant avro generator - nested", "testSchemaNested.avsc")
  testConstGen("Test constant avro generator - fields", "testSchemaFields.avsc")
  testConstGen("Test constant avro generator - array", "testSchemaArray.avsc")
  testConstGen("Test constant avro generator - union", "testSchemaUnion.avsc")
  testConstGen("Test constant avro generator - logical types", "testSchemaLogicalTypes.avsc")
}
