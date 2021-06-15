package com.github.andrewinci.impl

import com.github.andrewinci.core.FieldGenerator
import com.github.andrewinci.core.FieldGeneratorException
import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericData
import java.nio.ByteBuffer

class ConstFieldGen(
    str: String = "",
    int: Int = 0,
    decimal: BigDecimal = 0.1,
    boolean: Boolean = true,
    bytes: Array[Byte] = Array[Byte](1.toByte, 2.toByte, 3, 4)
) extends FieldGenerator {
  override def getGenerator(fieldName: String): Option[FieldGenerator] = Some(this)

  override def generate(schema: Schema): Either[FieldGeneratorException, Any] = schema.getType match {
    case Type.RECORD | Type.ARRAY | Type.MAP | Type.UNION | Type.FIXED =>
      Left(new FieldGeneratorException(s"Unable to generate a field of type ${schema.getType.getName}"))
    case Type.ENUM    => Right(new GenericData.EnumSymbol(schema, schema.getEnumSymbols.get(0)))
    case Type.BYTES   => Right(ByteBuffer.wrap(bytes))
    case Type.STRING  => Right(str)
    case Type.INT     => Right(int)
    case Type.LONG    => Right(int)
    case Type.FLOAT   => Right(decimal.toFloat)
    case Type.DOUBLE  => Right(decimal.toDouble)
    case Type.BOOLEAN => Right(boolean)
    case Type.NULL    => Right(null)
  }
}
