package com.twitter.zipkin.storage.cassandra

import com.datastax.driver.core.Cluster
import com.twitter.app.App
import com.twitter.zipkin.cassandra.CassieSpanStoreFactory
import com.twitter.zipkin.storage.SpanStoreSpec
import java.util.Collections
import org.cassandraunit.CassandraCQLUnit
import org.cassandraunit.dataset.CQLDataSet
import org.junit.ClassRule

object CassieSpanStoreSpec {
  // Avoid conflicts with thrift 0.5
  System.setProperty("cassandra.start_rpc", "false")

  // Scala cannot generate fields with public visibility, so use a def instead.
  @ClassRule def cassandra = new CassandraCQLUnit(new CQLDataSet() {
    override def isKeyspaceDeletion = true

    override def getKeyspaceName = "zipkin"

    override def isKeyspaceCreation = true

    override def getCQLStatements = Collections.emptyList()
  })
}

class CassieSpanStoreSpec extends SpanStoreSpec {

  object TestStore extends App with CassieSpanStoreFactory

  // unused currently
  TestStore.main(Array("-zipkin.store.cassandra.dest", "127.0.0.1:9142"))
  override lazy val store = TestStore.newCassandraStore(
    Cluster.builder().addContactPoint("127.0.0.1").withPort(9142).build()
  )

  override def clear = {
    // class rule clears the keyspace
  }
}
