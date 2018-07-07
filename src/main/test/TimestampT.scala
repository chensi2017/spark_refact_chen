import java.text.SimpleDateFormat
import java.util.Date

object TimestampT {
  def main(args: Array[String]): Unit = {
    val formate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println(formate.format(new Date(1530809400000l)))
    println(formate.format(new Date(1530809405000l)))
    println(formate.format(new Date(1530809415000l)))
  }
}
