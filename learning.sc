import scala.util.matching.Regex

val testString = "to universal, everything is 42"
val pattern: Regex = """.* ([\d]+).*""".r

// this line really call pattern.unapplySeq(testString)
val pattern(answerString) = testString
println(answerString)