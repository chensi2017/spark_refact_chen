import scala.collection.{immutable, mutable}

object TreeSetTest {
  def main(args: Array[String]): Unit = {
    val set = new mutable.TreeSet[Long]()
    set.add(3l)
    set.add(13l)
    set.add(5l)
    set.add(0l)
    set.add(7l)
//    println(set)
    val arr = Array(1,4,3,2,5)
    val res = arr.sortWith((a,b)=>(a>b)).foreach(println)
  }
}
