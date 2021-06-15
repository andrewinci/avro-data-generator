package com.github.andrewinci.core

class AvroGeneratorException(message: String, cause: Exception = null) extends Exception(message, cause)

class NotImplementedException(message: String) extends AvroGeneratorException(message)

class FieldGeneratorException(message: String) extends AvroGeneratorException(message)
