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

import com.datastax.driver.core.Cluster;
import com.twitter.app.App
import com.twitter.conversions.time._
import com.twitter.util.Await
import com.twitter.zipkin.cassandra.CassieSpanStoreFactory
import com.twitter.zipkin.common._
import com.twitter.zipkin.query.Trace
import com.twitter.zipkin.storage.util.SpanStoreValidator
import org.scalatest.{BeforeAndAfter, FunSuite}


class CassieSpanStoreTest extends FunSuite {

  object TestStore extends App with CassieSpanStoreFactory
  TestStore.main(Array("-zipkin.store.cassandra.keyspace", "test_keyspace"))

  var newSpanStore = TestStore.newCassandraStore(Cluster.builder().addContactPoint("localhost").build())


  test("validate") {
    // FakeCassandra doesn't honor sort order
    new SpanStoreValidator(newSpanStore, true).validate
  }

  // storage tests

//  test("getSpansByTraceId") {
//    spanStore = StorageBuilder(Cluster.builder().addContactPoint("localhost").build(), keyspace = "test_getspansbytraceid").apply()
//    Await.result(spanStore.storeSpan(span1))
//    val spans = Await.result(spanStore.getSpansByTraceId(span1.traceId))
//    assert(!spans.isEmpty)
//    assert(spans(0) === span1)
//  }
//
//  test("getSpansByTraceIds") {
//    spanStore = StorageBuilder(Cluster.builder().addContactPoint("localhost").build(), keyspace = "test_getspansbytraceids").apply()
//    Await.result(spanStore.storeSpan(span1))
//    val actual1 = Await.result(spanStore.getSpansByTraceIds(List(span1.traceId)))
//    assert(!actual1.isEmpty)
//
//    val trace1 = Trace(actual1(0))
//    assert(!trace1.spans.isEmpty)
//    assert(trace1.spans(0) === span1)
//
//    val span2 = Span(666, "methodcall2", spanId, None, List(ann2),
//      List(binaryAnnotation("BAH2", "BEH2")))
//    Await.result(spanStore.storeSpan(span2))
//    val actual2 = Await.result(spanStore.getSpansByTraceIds(List(span1.traceId, span2.traceId)))
//    assert(!actual2.isEmpty)
//
//    val trace2 = Trace(actual2(0))
//    val trace3 = Trace(actual2(1))
//    assert(!trace2.spans.isEmpty)
//    assert(trace2.spans(0) === span1)
//    assert(!trace3.spans.isEmpty)
//    assert(trace3.spans(0) === span2)
//  }
//
//  test("getSpansByTraceIds should return empty list if no trace exists") {
//    spanStore = StorageBuilder(Cluster.builder().addContactPoint("localhost").build(), keyspace = "test_getspansbytraceids_empty").apply()
//    val actual1 = Await.result(spanStore.getSpansByTraceIds(List(span1.traceId)))
//    assert(actual1.isEmpty)
//  }
//
//  test("all binary annotations are logged") {
//    spanStore = StorageBuilder(Cluster.builder().addContactPoint("localhost").build(), keyspace = "test_binary_annotations").apply()
//    val a_traceId = 1234L
//    val a1 = Annotation(1, "sr", Some(ep))
//    val a2 = Annotation(2, "ss", Some(ep))
//    val ba1 = binaryAnnotation("key1", "value1")
//    val ba2 = binaryAnnotation("key2", "value2")
//    val originalKeyNames = Set("key1", "key2")
//    val a_span1 = Span(a_traceId, "test", 345L, None, List(a1), Nil)
//    val a_span2 = Span(a_traceId, "test", 345L, None, Nil, List(ba1))
//    val a_span3 = Span(a_traceId, "test", 345L, None, Nil, List(ba2))
//    val a_span4 = Span(a_traceId, "test", 345L, None, List(a2), Nil)
//    val data = List(a_span1, a_span2, a_span3, a_span4)
//    for(s <- data) {
//      Await.result(spanStore.storeSpan(s))
//    }
//
//    val actual1 = Await.result(spanStore.getSpansByTraceIds(List(a_traceId)))
//    val trace1 = Trace(actual1(0))
//    val bAnnotations = trace1.spans(0).binaryAnnotations
//    val keyNames = bAnnotations map { _.key }
//    assert(bAnnotations.length === 2)
//    assert(keyNames.toSet === originalKeyNames)
//
//  }

//  test("set time to live on a trace and then get it") {
//    cassandraStorage = StorageBuilder(Cluster.builder().addContactPoint("localhost").build(), keyspace = "zkipkin").apply()
//    Await.result(cassandraStorage.storeSpan(span1))
//    Await.result(cassandraStorage.setTimeToLive(span1.traceId, 1234.seconds))
//    assert(Await.result(cassandraStorage.getTimeToLive(span1.traceId)) === 1234.seconds)
//  }

  // aggregates tests

//class CassandraAggregatesTest extends FunSuite with MockitoSugar with BeforeAndAfter {
//
//  val mockKeyspace = mock[Keyspace]
//  val mockAnnotationsCf = mock[ColumnFamily[String, Long, String]]
//  val mockDependenciesCf = mock[ColumnFamily[ByteBuffer, Long, thriftscala.Dependencies]]
//
//  def cassandraAggregates = CassandraAggregates(mockKeyspace, mockAnnotationsCf, mockDependenciesCf)
//
//  def column(name: Long, value: String) = new Column[Long, String](name, value)
//
//  val topAnnsSeq = Seq("finagle.retry", "finagle.timeout", "annotation1", "annotation2")
//  val topAnns = topAnnsSeq.zipWithIndex.map { case (ann, index) =>
//    index.toLong -> column(index, ann)
//  }.toMap.asJava
//
//  val serviceCallsSeq = Seq("parent1:10, parent2:20")
//  val serviceCalls = serviceCallsSeq.zipWithIndex.map {case (ann, index) =>
//    index.toLong -> column(index, ann)
//  }.toMap.asJava
//
//  test("Top Annotations") {
//    val agg = cassandraAggregates
//    val serviceName = "mockingbird"
//    val rowKey = agg.topAnnotationRowKey(serviceName)
//
//    when(mockAnnotationsCf.getRow(rowKey)).thenReturn(Future.value(topAnns))
//    assert(agg.getTopAnnotations(serviceName)() === topAnnsSeq)
//  }
//
//  test("Top KeyValue Annotations") {
//    val agg = cassandraAggregates
//    val serviceName = "mockingbird"
//    val rowKey = agg.topKeyValueRowKey(serviceName)
//
//    when(mockAnnotationsCf.getRow(rowKey)).thenReturn(Future.value(topAnns))
//    assert(agg.getTopKeyValueAnnotations(serviceName)() === topAnnsSeq)
//  }
//
//  test("storeTopAnnotations") {
//    agg.storeTopAnnotations(serviceName, topAnnsSeq).apply()
//    assert(agg.getTopAnnotations(serviceName).apply() === topAnnsSeq)
//  }
//
//  test("storeTopKeyValueAnnotations") {
//    agg.storeTopKeyValueAnnotations(serviceName, topAnnsSeq).apply()
//    assert(agg.getTopKeyValueAnnotations(serviceName).apply() === topAnnsSeq)
//  }
//
//  test("storeDependencies") {
//    val m1 = Moments(2)
//    val m2 = Moments(4)
//    val dl1 = DependencyLink(Service("tfe"), Service("mobileweb"), m1)
//    val dl3 = DependencyLink(Service("Gizmoduck"), Service("tflock"), m2)
//    val deps1 = Dependencies(Time.fromSeconds(0), Time.fromSeconds(0)+1.hour, List(dl1, dl3))
//
//    // ideally we'd like to retrieve the stored deps but FakeCassandra does not support
//    // the retrieval mechanism we use to get out dependencies.
//    // check this doesn't throw an exception
//    Await.result(agg.storeDependencies(deps1))
//  }
//
//  test("clobber old entries") {
//    val anns1 = Seq("a1", "a2", "a3", "a4")
//    val anns2 = Seq("a5", "a6")
//
//    agg.storeTopAnnotations(serviceName, anns1).apply()
//    assert(agg.getTopAnnotations(serviceName).apply() === anns1)
//
//    agg.storeTopAnnotations(serviceName, anns2).apply()
//    assert(agg.getTopAnnotations(serviceName).apply() === anns2)
//  }
//}

  // index tests

//class CassandraIndexTest extends FunSuite with BeforeAndAfter with MockitoSugar {
//
//  val mockKeyspace = mock[Keyspace]
//  var cassandraIndex: CassandraIndex = null
//
//  val ep = Endpoint(123, 123, "service")
//
//  def binaryAnnotation(key: String, value: String) =
//    BinaryAnnotation(key, ByteBuffer.wrap(value.getBytes), AnnotationType.String, Some(ep))
//
//  val spanId = 456
//  val ann1 = Annotation(1, "cs", Some(ep))
//  val ann2 = Annotation(2, "sr", None)
//  val ann3 = Annotation(2, "custom", Some(ep))
//  val ann4 = Annotation(2, "custom", Some(ep))
//
//  val span1 = Span(123, "methodcall", spanId, None, List(ann1, ann3),
//    List(binaryAnnotation("BAH", "BEH")))
//  val span2 = Span(123, "methodcall", spanId, None, List(ann2),
//    List(binaryAnnotation("BAH2", "BEH2")))
//  val span3 = Span(123, "methodcall", spanId, None, List(ann2, ann3, ann4),
//    List(binaryAnnotation("BAH2", "BEH2")))
//
//  val spanEmptySpanName = Span(123, "", spanId, None, List(ann1, ann2), List())
//  val spanEmptyServiceName = Span(123, "spanname", spanId, None, List(), List())
//
//  val mergedSpan = Span(123, "methodcall", spanId, None,
//    List(ann1, ann2), List(binaryAnnotation("BAH2", "BEH2")))
//
//  test("index and get span names") {
//    cassandraIndex.indexSpanNameByService(span1)()
//    assert(cassandraIndex.getSpanNames("service")() === Set(span1.name))
//  }
//
//  test("index and get service names") {
//    cassandraIndex.indexServiceName(span1)()
//    assert(cassandraIndex.getServiceNames() === Set(span1.serviceNames.head))
//  }
//
//  test("index only on annotation in each span with the same value") {
//    val mockAnnotationsIndex = mock[ColumnFamily[ByteBuffer, Long, Long]]
//    val batch = mock[BatchMutationBuilder[ByteBuffer, Long, Long]]
//
//    val cs = new CassandraIndex(mockKeyspace, null, null, null, null, mockAnnotationsIndex, null)
//    val col = Column[Long, Long](ann3.timestamp, span3.traceId)
//
//    when(mockAnnotationsIndex.batch).thenReturn(batch)
//    when(batch.execute).thenReturn(Future.Void)
//
//    cs.indexSpanByAnnotations(span3)
//
//    verify(batch, atLeastOnce()).insert(any[ByteBuffer], any[Column[Long, Long]])
//    verify(batch, times(1)).execute()
//  }
//
//  test("getTraceIdsByName") {
//    var ls = List[Long]()
//    //cassandra.storeSpan(span1)()
//    cassandraIndex.indexTraceIdByServiceAndName(span1)()
//    cassandraIndex.getTraceIdsByName("service", None, 0, 3)() foreach { m =>
//      assert(m.traceId === span1.traceId)
//    }
//    cassandraIndex.getTraceIdsByName("service", Some("methodname"), 0, 3)() foreach { m =>
//      assert(m.traceId === span1.traceId)
//    }
//  }
//
//  test("getTracesDuration") {
//    // no support in FakeCassandra for order and limit and it seems tricky to add
//    // so will mock the index instead
//
//    val durationIndex = new ColumnFamily[Long, Long, String] {
//      override def multigetRows(keys: JSet[Long], startColumnName: Option[Long], endColumnName: Option[Long], order: Order, count: Int) = {
//        if (!order.reversed) {
//          Future.value(Map(321L -> Map(100L -> Column(100L, "")).asJava).asJava)
//        } else {
//          Future.value(Map(321L -> Map(120L -> Column(120L, "")).asJava).asJava)
//        }
//      }
//    }
//
//    val cass = CassandraIndex(mockKeyspace, null, null, null, null, null, durationIndex)
//
//    val duration = cass.getTracesDuration(Seq(321L))()
//    assert(duration(0).traceId === 321L)
//    assert(duration(0).duration === 20)
//  }
//
//  test("get no trace durations due to missing data") {
//    // no support in FakeCassandra for order and limit and it seems tricky to add
//    // so will mock the index instead
//
//    val durationIndex = new ColumnFamily[Long, Long, String] {
//      override def multigetRows(keys: JSet[Long], startColumnName: Option[Long], endColumnName: Option[Long], order: Order, count: Int) = {
//        if (!order.reversed) {
//          Future.value(Map(321L -> Map(100L -> Column(100L, "")).asJava).asJava)
//        } else {
//          Future.value(Map(321L -> Map[Long, Column[Long,String]]().asJava).asJava)
//        }
//      }
//    }
//
//    val cass = CassandraIndex(mockKeyspace, null, null, null, null, null, durationIndex)
//
//    val duration = cass.getTracesDuration(Seq(321L))()
//    assert(duration.isEmpty)
//  }
//
//  test("getTraceIdsByAnnotation") {
//    cassandraIndex.indexSpanByAnnotations(span1)()
//
//    // fetch by time based annotation, find trace
//    val map1 = cassandraIndex.getTraceIdsByAnnotation("service", "custom", None, 0, 3)()
//    map1.foreach { m =>
//      assert(m.traceId === span1.traceId)
//    }
//
//    // should not find any traces since the core annotation doesn't exist in index
//    val map2 = cassandraIndex.getTraceIdsByAnnotation("service", "cs", None, 0, 3)()
//    assert(map2.isEmpty)
//
//    // should find traces by the key and value annotation
//    val map3 = cassandraIndex.getTraceIdsByAnnotation("service", "BAH",
//      Some(ByteBuffer.wrap("BEH".getBytes)), 0, 3)()
//    map3.foreach { m =>
//      assert(m.traceId === span1.traceId)
//    }
//  }
//
//  test("not index empty service name") {
//    cassandraIndex.indexServiceName(spanEmptyServiceName)
//    val serviceNames = cassandraIndex.getServiceNames()
//    assert(serviceNames.isEmpty)
//  }
//
//  test("not index empty span name") {
//    cassandraIndex.indexSpanNameByService(spanEmptySpanName)
//    val spanNames = cassandraIndex.getSpanNames(spanEmptySpanName.name)
//    assert(spanNames().isEmpty)
//  }
//}

}
