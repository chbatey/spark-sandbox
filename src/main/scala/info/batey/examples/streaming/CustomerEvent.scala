package info.batey.examples.streaming

import java.util.UUID

import com.datastax.driver.core.utils.UUIDs

case class CustomerEvent(customer_id: String, staff_id: String, store_type: String, group: String, content: String, time: UUID = UUIDs.timeBased(), event_type: String = "BUY")
