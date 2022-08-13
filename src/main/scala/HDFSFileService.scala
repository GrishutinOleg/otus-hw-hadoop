
import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import java.net.URI
import scala.::



object HDFSFileService extends App {
  private val conf = new Configuration()
  private val hdfsCoreSitePath = new Path("core-site.xml")
  private val hdfsHDFSSitePath = new Path("hdfs-site.xml")

  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)

  private val fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), conf)

  def saveFile(filepath: String): Unit = {
    val file = new File(filepath)
    val out = fileSystem.create(new Path(file.getName))
    val in = new BufferedInputStream(new FileInputStream(file))
    var b = new Array[Byte](1024)
    var numBytes = in.read(b)
    while (numBytes > 0) {
      out.write(b, 0, numBytes)
      numBytes = in.read(b)
    }
    in.close()
    out.close()
  }

  def removeFile(filename: String): Boolean = {
    val path = new Path(filename)
    fileSystem.delete(path, true)
  }

  def getFile(filename: String): InputStream = {
    val path = new Path(filename)
    fileSystem.open(path)
  }

  def createFolder(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }

  val filessrc = new Path("\\stage")

  val test1 = createFolder("\\ods")

  //val test4 = new Path("\\stage\\date=2020-12-03\\part-0001.csv")
  //val test2 = fileSystem.getFileStatus(test4)

  val lstfolders = fileSystem.listLocatedStatus(filessrc)

  while (lstfolders.hasNext) {
    val folderpath = lstfolders.next().getPath
    println(folderpath)
    val lstfiles = fileSystem.listFiles(folderpath, true)
    val folderpathstring = folderpath.toString
    while (lstfiles.hasNext) {
      val filepath = lstfiles.next().getPath

      println(filepath)
      val filepathstring = filepath.toString
      //println(filepathstring)
      //println(filepathstring.getClass)
      val isfilepathstring = filepathstring.endsWith(".csv")
      println(isfilepathstring)
      if (isfilepathstring)
        {
          val creatingfolder = createFolder(folderpathstring.replace("stage", "ods"))
          val removingfile = fileSystem.rename(filepath,
          new Path(filepathstring.replace("stage", "ods"))
          )
        }
      else {val isremovingfile = removeFile(filepathstring)}
    }
    //println(lstfolders1.next())
  }

  val filesdst = new Path("\\ods")

  val lstdestfolders = fileSystem.listLocatedStatus(filesdst)



  while (lstdestfolders.hasNext) {
    val folderdestpath = lstdestfolders.next().getPath
    println(folderdestpath)
    val lstdestfiles = fileSystem.listFiles(folderdestpath, true)
    val folderpathstring = folderdestpath.toString
    //val lstdestfiles2 = lstdestfiles.toString

    //var clearfilelist = Nil
    while (lstdestfiles.hasNext) {
      val filedestpath = lstdestfiles.next().getPath
      println(filedestpath)
      val filedestpathstr = filedestpath.toString
      if (!filedestpathstr.endsWith("part-0000.csv")) {
        val uniting = fileSystem.concat(
          new Path(folderpathstring ++ "/part-0000.csv")
          ,Array(filedestpath))
        val deleting = removeFile(filedestpathstr)
      }
      //val clearfilelist = clearfilelist :: filedestpath

    }
    //println(clearfilelist)

  }



  /*
  val test16 = createFolder("hdfs://localhost:9000/test/date=2020-12-03")

  val test17 = fileSystem.copyFromLocalFile(
    new Path("hdfs://localhost:9000/stage/date=2020-12-03/part-0001.csv"),
    new Path("hdfs://localhost:9000/test/date=2020-12-03/part-0001.csv") )

  val test18 = fileSystem.copyFromLocalFile(
    new Path("hdfs://localhost:9000/stage/date=2020-12-03/part-0000.csv"),
    new Path("hdfs://localhost:9000/test/date=2020-12-03/part-0000.csv") )

   */




  //println(lstfolders1)
  /*

  val test21 = new Path("hdfs://localhost:9000/test/stage/date=2020-12-03/part-0000.csv")

  val test22 = Array(
    new Path("hdfs://localhost:9000/test/stage/date=2020-12-03/part-0001.csv")
    //,new Path("hdfs://localhost:9000/test/stage/date=2020-12-03/part-0000.csv")
    ,new Path("hdfs://localhost:9000/test/stage/date=2020-12-03/part-0002.csv")
  )

  val test23 = fileSystem.concat(test21, test22)

   */
/*
  val test24 = fileSystem.rename(
    new Path("hdfs://localhost:9000/stage/date=2020-12-03/part-0001.csv"),
    new Path("hdfs://localhost:9000/test/part-0001.csv")
  )
*/

}
