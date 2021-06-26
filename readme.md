# avro-data-generator

## Get started

Update the `build.sbt` with the following:
```scala
resolvers += "jitpack" at "https://jitpack.io",
libraryDependencies += "com.github.andrewinci" % "avro-data-generator" % "2.0.0"
```

## Development

Use `sbt scalafmtAll` to format the code.

Use `sbt test` to run the tests

Use `sbt IntegrationTest / test` to run the integration tests


## TODO
- [x] JsonGen - Bytes
- [ ] Map types
- [ ] Fixed types