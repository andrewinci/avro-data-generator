package com.github.andrewinci.generators

import com.github.andrewinci.core.AvroFieldGeneratorLeaf
import com.github.andrewinci.core.AvroFieldGeneratorNode
import com.github.andrewinci.core.EmptyAvroFieldGenerator
import com.github.andrewinci.core.FieldGeneratorException
import org.apache.avro.Schema

object AvroFieldGenerator {
  import com.github.andrewinci.core.AvroFieldGenerator

  type AvroFieldGeneratorMap = Map[String, (Schema) => Either[FieldGeneratorException, Any]]

  /** Given a map field path (i.e. field1.nested1.nested2...) to field generator, build
    * an avro field generator
    * @param map map field path to field gen
    */
  def fromMap(map: AvroFieldGeneratorMap): AvroFieldGenerator = {
    val generatorsMap = map
      .map(m => m._1.split('.') -> m._2)
      .map(m =>
        m._1.head -> m._1.tail.foldRight[AvroFieldGenerator](AvroFieldGeneratorLeaf(m._2))((a, b) =>
          AvroFieldGeneratorNode(a, b)
        )
      )
    new AvroFieldGenerator {
      override def getGenerator(fieldName: String): Option[AvroFieldGenerator] = generatorsMap.get(fieldName)

      override def generate(schema: Schema): Either[FieldGeneratorException, Any] =
        EmptyAvroFieldGenerator.generate(schema)
    }
  }
}
