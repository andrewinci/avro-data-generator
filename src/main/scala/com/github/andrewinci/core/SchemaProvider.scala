package com.github.andrewinci.core

import org.apache.avro.Schema

trait SchemaProvider {
  /**
   * Retrieve the latest schema available
   * for the provided subject
   *
   * @param subject the subject to retrieve teh schema for
   * @return the schema if the subject exists
   */
  def getLatestSchema(subject: String): Option[Schema]

  /**
   * Retrieve a specific schema version
   *
   * @param subject the subject to retrieve teh schema for
   * @param version schema version
   * @return the schema if the subject and the version exist
   */
  def getSchema(subject: String, version: Int): Option[Schema]
}
