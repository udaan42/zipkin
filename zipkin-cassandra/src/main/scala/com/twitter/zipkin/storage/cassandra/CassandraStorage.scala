/*
 * Copyright 2012 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.zipkin.storage.cassandra

import com.twitter.cassie._
import com.twitter.cassie.codecs.Codec
import com.twitter.conversions.time._
import com.twitter.ostrich.stats.Stats
import com.twitter.util.{Await, Duration, Future, FuturePool}
import com.twitter.zipkin.common.Span
import com.twitter.zipkin.conversions.thrift._
import com.twitter.zipkin.thriftscala
import com.twitter.zipkin.storage.Storage
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

case class CassandraStorage(
  repository: org.twitter.zipkin.storage.cassandra.Repository,
  readBatchSize: Int,
  dataTimeToLive: Duration,
  spanCodec: Codec[thriftscala.Span]
) extends Storage {

  def close() {
    repository.close()
  }

  // storing the span in the traces cf
  private val CASSANDRA_STORE_SPAN = Stats.getCounter("cassandra_storespan")

  // read the trace
  private val CASSANDRA_GET_TRACE = Stats.getCounter("cassandra_gettrace")

  // trace exist call
  private val CASSANDRA_TRACE_EXISTS = Stats.getCounter("cassandra_traceexists")

  // trace is too big!
  private val CASSANDRA_GET_TRACE_TOO_BIG = Stats.getCounter("cassandra_gettrace_too_big")

  private val WRITE_REQUEST_COUNTER = Stats.getCounter("cassandra.write_request_counter")

  // there's a bug somewhere that creates massive traces. if we try to
  // read them without a limit we run the risk of blowing up the memory used in
  // cassandra. so instead we limit it and won't return it. hacky.
  private val TRACE_MAX_COLS = 100000

  private val pool = FuturePool.unboundedPool

  def storeSpan(span: Span): Future[Unit] = {
    CASSANDRA_STORE_SPAN.incr
    WRITE_REQUEST_COUNTER.incr()

    repository
      .storeSpan(span.traceId, createSpanColumnName(span), spanCodec.encode(span.toThrift), dataTimeToLive.inSeconds)

    Future.Unit
  }

  def setTimeToLive(traceId: Long, ttl: Duration): Future[Unit] = {
    // @todo implement
    Future.Unit
//    val rowFuture = getSpansByTraceId(traceId)
//
//    // fetch each col for trace, change ttl and reinsert
//    // note that we block here
//    Await.result(rowFuture).values().asScala.foreach { value =>
//      repository.storeSpan(value.traceId, createSpanColumnName(value), value.toThrift, ttl)
//    }
  }

  def getTimeToLive(traceId: Long): Future[Duration] = {
    // @todo implement
    Future(dataTimeToLive)
  }

  /**
   * Finds traces that have been stored from a list of trace IDs
   *
   * @param traceIds a List of trace IDs
   * @return a Set of those trace IDs from the list which are stored
   */

  def tracesExist(traceIds: Seq[Long]): Future[Set[Long]] = {
    CASSANDRA_TRACE_EXISTS.incr
    // @todo  honour readBatchSize
    pool {
      repository
        .tracesExist(traceIds.toArray.map(Long.box))
        .get
        .map(_.asInstanceOf[Long])
        .toSet
    }
  }

  /**
   * Fetches traces from the underlying storage. Note that there might be multiple
   * entries per span.
   */
  def getSpansByTraceId(traceId: Long): Future[Seq[Span]] = {
    getSpansByTraceIds(Seq(traceId)).map {
      _.head
    }
  }

  def getSpansByTraceIds(traceIds: Seq[Long]): Future[Seq[Seq[Span]]] = {
    CASSANDRA_GET_TRACE.incr
    // @todo  honour readBatchSize
    pool {
      val spans = repository
        .getSpansByTraceIds(traceIds.toArray.map(Long.box))
        .getUninterruptibly
        .map(row => spanCodec.decode(row.getBytes("value")).toSpan)
        .toSeq
        .groupBy(_.traceId)
        .toMap

      traceIds.map(traceId => spans.get(traceId)).filter(_.isDefined).map(_.get).toSeq
    }
  }

  def getDataTimeToLive: Int = dataTimeToLive.inSeconds

  /*
  * Helper methods
  * --------------
  */

  /**
   * One span will be logged by two different machines we want to store it in cassandra
   * without having to do a read first to do so we create a unique column name
   */
  private def createSpanColumnName(span: Span) : String = {
    // TODO make into a codec?
    span.id.toString + "_" + span.annotations.hashCode + "_" + span.binaryAnnotations.hashCode
  }
}
