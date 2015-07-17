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
package com.twitter.zipkin.cassandra

import com.datastax.driver.core.Cluster;
import com.twitter.cassie.codecs.Codec
import com.twitter.conversions.time._
import com.twitter.util.Duration
import com.twitter.zipkin.thriftscala
import com.twitter.zipkin.builder.Builder
import com.twitter.zipkin.storage.cassandra.{CassandraStorage, ScroogeThriftCodec, SnappyCodec}
import com.twitter.zipkin.storage.Storage

case class StorageBuilder(
  cluster: Cluster,
  dataTimeToLive: Duration = 7.days,
  spanCodec: Codec[thriftscala.Span] = new SnappyCodec(new ScroogeThriftCodec[thriftscala.Span](thriftscala.Span)),
  readBatchSize: Int = 500,
  keyspace: String = "zipkin"
) extends Builder[Storage] {

  def apply() = {
    val repository = new org.twitter.zipkin.storage.cassandra.Repository(keyspace, cluster)
    CassandraStorage(repository, readBatchSize, dataTimeToLive, spanCodec)
  }
}
