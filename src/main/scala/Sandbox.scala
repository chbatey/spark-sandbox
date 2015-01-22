import org.json4s._
import org.json4s.ext.UUIDSerializer
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}

object Sandbox extends App {
  implicit val formats = Serialization.formats(NoTypeHints) + UUIDSerializer

  val customerEvent = CustomerEvent("joe", "chris", "WEB", "NEW_CUSTOMER", "lots of fancy content")

  println(write(customerEvent))

}
