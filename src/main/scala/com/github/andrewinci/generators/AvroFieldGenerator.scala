package com.github.andrewinci.generators

import com.github.andrewinci.core.FieldGeneratorException
import com.github.andrewinci.generators.helpers.AvroFieldGeneratorLeaf
import com.github.andrewinci.generators.helpers.AvroFieldGeneratorNode
import com.github.andrewinci.generators.helpers.Compose.compose
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.avro.Schema
import scala.util.Try

object AvroFieldGenerator {
  import com.github.andrewinci.core.AvroFieldGenerator

  type FieldNameToAvroGen = (String, (Schema) => Either[FieldGeneratorException, Any])

  /** Given a map field path (i.e. field1.nested1.nested2...) to field generator, build
    * an avro field generator
    * @param map map field path to field gen
    */
  def fromMap(map: FieldNameToAvroGen*): AvroFieldGenerator = map
    .map(m => m._1.split('.') -> m._2)
    .map(m => m._1.foldRight[AvroFieldGenerator](AvroFieldGeneratorLeaf(m._2))((a, b) => AvroFieldGeneratorNode(a, b)))
    .reduce((g1, g2) => compose(g1, g2))

  def fromJson(jsonString: String): Either[Throwable, AvroFieldGenerator] = Try {
    new ObjectMapper().readTree(jsonString)
  }.map(j => JsonAvroFieldGenerator(j)).toEither
}
