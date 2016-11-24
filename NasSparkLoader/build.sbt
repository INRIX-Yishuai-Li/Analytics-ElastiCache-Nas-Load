name := "NasSparkLoader"

version := "1.0"

scalaVersion := "2.10.4"

val sparkVersion = "1.6.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  //"org.apache.spark" %% "spark-core" % sparkVersion ,
  "org.scalactic" %% "scalactic" % "3.0.0" % Test,
  "org.scalatest" %% "scalatest" % "3.0.0" % Test
)

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Artima Maven Repository" at "http://repo.artima.com/releases"
)

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
