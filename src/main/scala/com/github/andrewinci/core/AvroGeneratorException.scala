package com.github.andrewinci.core

class AvroGeneratorException(message: String, cause: Option[Throwable] = None) extends Throwable(message, cause.orNull)

class NotImplementedException(message: String) extends AvroGeneratorException(message)

class FieldGeneratorException(message: String, cause: Option[Throwable] = None)
    extends AvroGeneratorException(message, cause)
