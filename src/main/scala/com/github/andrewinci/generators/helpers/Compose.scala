package com.github.andrewinci.generators.helpers

import com.github.andrewinci.core.AvroFieldGenerator
import com.github.andrewinci.core.FieldGeneratorException
import org.apache.avro.Schema

object Compose {

  def compose(gen1: AvroFieldGenerator, gen2: AvroFieldGenerator): AvroFieldGenerator = new AvroFieldGenerator {

    override def getGenerator(fieldName: String): Option[AvroFieldGenerator] =
      (gen1.getGenerator(fieldName), gen2.getGenerator(fieldName)) match {
        case (Some(g1), Some(g2)) => Some(compose(g1, g2))
        case (g1, g2)             => g1.orElse(g2)
      }

    override def generate(schema: Schema): Either[FieldGeneratorException, Any] =
      (gen1.generate(schema), gen2.generate(schema)) match {
        case (Right(g), _) => Right(g)
        case (_, Right(g)) => Right(g)
        case (l, _)        => l
      }
  }
}
