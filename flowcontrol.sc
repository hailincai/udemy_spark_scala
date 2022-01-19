// fib

var prev = 0
var cur = 0
for (x <- 0 to 9){
  x match {
    case 0 => {
      print("0 ")
    }
    case 1 => {
      print("1 ")
      prev = 0
      cur = 1
    }
    case _ => {
      print(s"${prev + cur} ")
      val tmp = prev + cur
      prev = cur
      cur = tmp
    }
  }
}