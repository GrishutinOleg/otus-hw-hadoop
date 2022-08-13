
import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import java.net.URI



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

  val initialsrs: Array[String] = Array(
    "C:\\otus_spark_developer\\hadoop_course_homework-master\\hadoop_course_homework-master\\hw1\\sample_data\\stage\\date=2020-12-01\\part-0000.csv"
    ,"C:\\otus_spark_developer\\hadoop_course_homework-master\\hadoop_course_homework-master\\hw1\\sample_data\\stage\\date=2020-12-03\\part-0000.csv"
    ,"C:\\otus_spark_developer\\hadoop_course_homework-master\\hadoop_course_homework-master\\hw1\\sample_data\\stage\\date=2020-12-03\\part-0001.csv"
    ,"C:\\otus_spark_developer\\hadoop_course_homework-master\\hadoop_course_homework-master\\hw1\\sample_data\\stage\\date=2020-12-03\\part-0002.csv"
  )
  val initialfolder = new Path("C:\\otus_spark_developer\\hadoop_course_homework-master\\hadoop_course_homework-master\\hw1\\sample_data\\stage")
  val desthdfsfolder = new Path("C:\\hadoop\\data\\datanode")

  //val rescoping = fileSystem copyFromLocalFile(initialfolder, desthdfsfolder)
  val filessrc = new Path("\\stage")

  val filesdst = new Path("\\ods")

  val test1 = createFolder("\\ods")
  val test4 = new Path("\\stage\\date=2020-12-03\\part-0001.csv")
  val test2 = fileSystem.getFileStatus(test4)
  //println(test2)
  //println(test2.isDirectory)
  //val test5 = test2.path
  //println(test2.getPath)
  //println(test5.getClass)

  val lstfolders1 = fileSystem.listLocatedStatus(filessrc)

  while (lstfolders1.hasNext) {
    val test11 = lstfolders1.next().getPath
    println(test11)
    val lstfilestest1 = fileSystem.listFiles(test11, true)
    val test14 = test11.toString
    while (lstfilestest1.hasNext) {
      val test7 = lstfilestest1.next().getPath
      //println(lstfilestest1.next().getPath)
      println(test7)
      val test8 = test7.toString
      //println(test8)
      //println(test8.getClass)
      val test9 = test8.endsWith(".csv")
      println(test9)
      if (test9) println("new path is  " ++ test14.replace("stage", "ods"))
    }
    //println(lstfolders1.next())
  }






  //println(lstfolders1)

}
