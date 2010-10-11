import sbt._

class ScadoopProject(info: ProjectInfo) extends DefaultProject(info) {
  val mavenRepo = "maven" at "http://repo2.maven.org/maven2"
  val apacheRepo = "apache" at "http://repository.apache.org"
  val jbossReop = "jboss" at "http://repository.jboss.com/maven2"

  val scalatest = "org.scalatest" % "scalatest" % "1.2"
}
