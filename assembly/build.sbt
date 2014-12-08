lazy val libDirectory = settingKey[File]("lib directory")

libDirectory <<= baseDirectory(_ / "lib")

lazy val collectJars = TaskKey[Unit]("collect-jars", "Collect all jars in lib")

collectJars <<= (fullClasspath in Compile, libDirectory) map { (paths, lib) =>
  val files = paths.files.filter(path => path.exists && path.isFile)
  files.foreach( f => IO.copyFile(f, lib / f.getName))
}

collectJars <<= collectJars.dependsOn(compile in Compile)

cleanFiles <+= libDirectory