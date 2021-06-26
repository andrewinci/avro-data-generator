# avro-data-generator
![Coveralls](https://img.shields.io/coveralls/github/andrewinci/avro-data-generator)
![Snyk Vulnerabilities for GitHub Repo](https://img.shields.io/snyk/vulnerabilities/github/andrewinci/avro-data-generator)
![CircleCI](https://img.shields.io/circleci/build/github/andrewinci/avro-data-generator)
[![](https://jitpack.io/v/andrewinci/avro-data-generator.svg)](https://jitpack.io/#andrewinci/avro-data-generator)

Generic avro records generators

The core components of this library are the `AvroGenerator` and `AvroFieldGenerator`.

The `AvroFieldGenerator` define how to generate a specific field and define a tree structure.
It is possible to compose multiple Field generators to extend the number of fields/types the generator
is able to generate.

The `AvroGenerator` depends on `AvroFieldGenerator` and it's generating the final Avro record
iterating over the fields in the Avro schema.

## Get started

Update the `build.sbt` with the following:
```scala
resolvers += "jitpack" at "https://jitpack.io",
libraryDependencies += "com.github.andrewinci" % "avro-data-generator" % "<latest-tag>"
```

### Generate a generic record with predefined values
Create an instance of the constant field generator setting a predefined value
for each field type. See full example at [ConstGenerator - Fields](src/main/scala/com/github/andrewinci/examples/ConstGenerator.scala)
```scala
val schema = new Schema.Parser().parse(avroSchema)
val fieldGenerator = AvroFieldGenerators.constantFieldsGen(str = "Constant value for every string")
val result = AvroGenerator(fieldGenerator).generateRecord(schema)
```

### Generate a generic record from a Json
Generate a `GenericRecord` providing all the field values in a json string. 
See full example at [JsonGenerator - Fields](src/main/scala/com/github/andrewinci/examples/JsonGenerator.scala)
```scala
val schema = new Schema.Parser().parse(avroSchema)

val result: Either[Throwable, GenericRecord] = AvroFieldGenerators
  .fromJson(values)
  .map(AvroGenerator(_))
  .flatMap(_.generateRecord(schema))
```

### Compose generators
Extend field generators to include more fields by composition.
The `AvroGenerator` will try to build every field in order.  
i.e. if `g = g1 compose g2` `g` will firs try to build a field with `g1` and only if it fails 
fallback on `g2`. See full example at [JsonConstGenerator - Fields](src/main/scala/com/github/andrewinci/examples/JsonConstGenerator.scala)

```scala
val schema = new Schema.Parser().parse(avroSchema)

val result: Either[Throwable, GenericRecord] = AvroFieldGenerators
    .fromJson(values)
    .map(compose(_, AvroFieldGenerators.constantFieldsGen()))
    .map(AvroGenerator(_))
    .flatMap(_.generateRecord(schema))
```

## Development

Use `sbt scalafmtAll` to format the code.

Use `sbt test` to run the tests

Use `sbt IntegrationTest / test` to run the integration tests


## TODO
- [x] JsonGen - Bytes
- [ ] Map types
- [ ] Fixed types