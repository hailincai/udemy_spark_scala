def sqaureIt(x: Int): Int = {
  x * x
}

def curbeIt(x: Int) : Int = { x * x * x }

def transformInt(x: Int, f: Int => Int ) = {
  f(x)
}

transformInt(3, x => x * x * x)

def stringUppercase(s: String) : String = {
  s.toUpperCase();
}

val func1: String => String = x  => x.toUpperCase()