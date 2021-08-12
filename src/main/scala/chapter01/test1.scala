package chapter01

object test1 {
  def main(args: Array[String]): Unit = {
    def func2(i: Int)(s: String)(c: Char): Boolean = {
      if (i == 0 && s == "" && c == '0') true else false
    }
    println(func2(0)("")('0'))
    println(func2(23)("")('1'))




  }
}