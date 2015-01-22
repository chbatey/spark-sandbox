import java.util.UUID

import com.datastax.driver.core.utils.UUIDs

case class CustomerEvent(customerid: String, staffid: String, storetype: String, group: String, content: String, time: UUID = UUIDs.timeBased())
