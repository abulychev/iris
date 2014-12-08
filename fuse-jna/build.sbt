name := "fuse-jna"

organization := "fuse-jna"

version := "1.0.3"

libraryDependencies += "net.java.dev.jna" % "jna" % "3.5.2"

exportJars := true

lazy val fuseJnaDirectory = settingKey[File]("fuse-jna source dir")

fuseJnaDirectory <<= baseDirectory(_ / "downloaded-sources")

lazy val scriptFile = settingKey[File]("cloning script")

scriptFile <<= baseDirectory(_ / "clone.sh")

lazy val git = taskKey[Unit]("Cloning sources from git")

git := {
  s"sh ${scriptFile.value.getAbsolutePath} ${fuseJnaDirectory.value.getAbsolutePath}" !
}

unmanagedSourceDirectories in Compile <+= fuseJnaDirectory(_ / "src")

compile in Compile <<= (compile in Compile).dependsOn(git)

cleanFiles <+= fuseJnaDirectory