package com.github.andrewinci.generators

import com.github.andrewinci.core.AvroFieldGenerator
import com.github.andrewinci.core.FieldGeneratorException
import com.github.andrewinci.generators.helpers.AvroFieldGeneratorHelper.avroEnumPicker
import org.apache.avro.LogicalTypes.Decimal
import org.apache.avro.Conversions
import org.apache.avro.Schema
import org.apache.avro.Schema.Type

import java.nio.ByteBuffer

class ConstAvroFieldGen(
    constStr: String = "",
    constInt: Int = 0,
    constDecimal: BigDecimal = 0.1,
    constBoolean: Boolean = true,
    constBytes: Array[Byte] = Array[Byte](1.toByte, 2.toByte, 3, 4)
) extends AvroFieldGenerator {
  override def getGenerator(fieldName: String): Option[AvroFieldGenerator] = Some(this)

  override def generate(schema: Schema): Either[FieldGeneratorException, Any] =
    Option(schema.getLogicalType) match {
      // not a logical type
      case None =>
        schema.getType match {
          case Type.RECORD | Type.ARRAY | Type.MAP | Type.UNION | Type.FIXED =>
            Left(new FieldGeneratorException(s"Unable to generate a field for complex type ${schema.getType.getName}"))
          // only primitive types are supported
          case Type.ENUM    => avroEnumPicker(schema)(_.headOption)
          case Type.BYTES   => Right(ByteBuffer.wrap(constBytes))
          case Type.STRING  => Right(constStr)
          case Type.INT     => Right(constInt)
          case Type.LONG    => Right(constInt.toLong)
          case Type.FLOAT   => Right(constDecimal.toFloat)
          case Type.DOUBLE  => Right(constDecimal.toDouble)
          case Type.BOOLEAN => Right(constBoolean)
          case Type.NULL    => Right(null)
        }
      // handle logical types
      case Some(logicalType) =>
        logicalType match {
          case decimal: Decimal =>
            Right(new Conversions.DecimalConversion().toBytes(constDecimal.bigDecimal, schema, decimal))
          case _ =>
            Left(
              new FieldGeneratorException(
                s"Unable to generate a field for logical type ${schema.getType.getName}:${logicalType.getName}"
              )
            )
        }
    }
}
