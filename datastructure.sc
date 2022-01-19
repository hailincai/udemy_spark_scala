// Turple
// Immutable Lists
val captainStuff = ("Priced", "Enterprise-D", "NCC-1701-D")
println(captainStuff)

// Refer to the individual fields with a ONE-BASED index
println(captainStuff._1)
println(captainStuff._2)
println(captainStuff._3)

//val turple2 = "Priced" -> "Enterprised-D"

//
val shipList = List("Enterprise", "Defiant", "Voyager")
// zero based
println(shipList(1))

for (ship <- shipList) {println(ship)}

val backwardShips = shipList.map((ship) => ship.reverse)
println(backwardShips)

// reduce() to combine togather list
val numberList = List(1, 2, 3, 4, 5)
val sum = numberList.reduce((x, y) => x + y)

val moreNumbers = List(6,7,8)
val lotsOfNumbers = numberList ++ moreNumbers
lotsOfNumbers :+ 10
10 +: lotsOfNumbers

// Map
var shipMap = Map("Krik" -> "Enterprise", "Priced" -> "Enterprise-D")
shipMap = shipMap + ("Sisko" -> "Deep Space Nine")
println(shipMap("Krik"))
val missingShip = util.Try(shipMap("a")) getOrElse "Unknown"

for (x <- 1 to 20){
  if (x % 3 == 0){
    print(s"${x} ")
  }
}
println("")

val listOneToTwenty = 1 to 20
val even3List = listOneToTwenty.filter((x) => (x % 3 == 0))