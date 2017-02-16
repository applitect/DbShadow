organization := "com.github.applitect"

name := "dbshadow"

version := "0.0.1"

// Target Java 8 only
javacOptions ++= Seq("-source", "1.8")

initialize := {
  val _ = initialize.value
  if (sys.props("java.specification.version") != "1.8")
    sys.error("Java 8 is required for this project.")
}

bashScriptExtraDefines += """addJava "-Dlog4j.configuration=file://${app_home}/../conf/log4j.xml""""

libraryDependencies ++= Seq(
    "commons-cli"  % "commons-cli" % "1.3.1",
    "org.hibernate" % "hibernate-entitymanager" % "5.0.2.Final",
    "org.hibernate" % "hibernate-core" % "5.0.2.Final",
    "org.hibernate" % "hibernate-java8" % "5.0.2.Final",
    "org.hibernate" % "hibernate-hikaricp" % "5.0.2.Final",
    "com.zaxxer" % "HikariCP" % "2.4.5",
    "org.apache.commons" % "commons-lang3" % "3.4",
    "mysql" % "mysql-connector-java" % "5.1.36",
    "hsqldb" % "hsqldb" % "1.8.0.10",
// oracle doesn't provide their driver in standard maven repos
//  "com.oracle" % "ojdbc" % "11.2.0.3",
    "com.github.jsqlparser" % "jsqlparser" % "0.9.5",
    "org.apache.logging.log4j" % "log4j-api" % "2.5",
    "org.apache.logging.log4j" % "log4j-core" % "2.5",
    "org.hsqldb" % "hsqldb" % "2.3.1" % "test",
    "org.junit.jupiter" % "junit-jupiter-api" % "5.0.0-M3" % "test",
    "org.junit.platform" % "junit-platform-runner" % "1.0.0-M3" % "test",
    "org.junit.jupiter" % "junit-jupiter-engine" % "5.0.0-M3" % "test",
    "org.junit.vintage" % "junit-vintage-engine" % "4.12.0-M3" % "test",
    "com.novocode" % "junit-interface" % "0.11" % "test"
)

resolvers ++= Seq(
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    "Maven Central" at "http://repo1.maven.org/maven2"
)

// Remove scala dependency for pure Java app
autoScalaLibrary := false

// Remove the scala version from the generated/published artifact
crossPaths := false

publishMavenStyle := true

EclipseKeys.projectFlavor := EclipseProjectFlavor.Java

enablePlugins(JavaAppPackaging)
