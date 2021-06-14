# avro-data-generator

## Get started

Add `sbt-github-packages` to the `project/plugin.sbt`:
```scala
addSbtPlugin("com.codecommit" % "sbt-github-packages" % "0.5.3")
```

Update the `build.sbt` with the following:
```scala
githubTokenSource := TokenSource.GitConfig("github.token") || TokenSource.Environment("GITHUB_TOKEN"),
externalResolvers += "avro-data-generator" at "https://maven.pkg.github.com/andrewinci/avro-data-generator",
libraryDependencies += "com.github.andrewinci" %% "avro-data-generator" % "<latest version>"
```