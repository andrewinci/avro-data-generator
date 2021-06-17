package com.github.andrewinci.generators.helpers

import com.github.andrewinci.core.FieldGeneratorException
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import scala.collection.JavaConverters._

object AvroFieldGeneratorHelper {

  def avroEnumPicker(
      schema: Schema
  )(picker: Seq[String] => Option[String]): Either[FieldGeneratorException, GenericData.EnumSymbol] = {
    val symbols = schema.getEnumSymbols.asScala
    picker(symbols)
      .filter(symbols.contains)
      .map(new GenericData.EnumSymbol(schema, _))
      .toRight(new FieldGeneratorException(s"Unable to set a value for ${schema.getName}"))
  }

}
