/*
 * Copyright 2012 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.twitter.zipkin.storage;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

public interface Storage {

  /**
   * Close the storage
   */
  void close();

  /**
   * Store the span in the underlying storage for later retrieval.
   * @return a future for the operation
   */
  Future<Void> storeSpan(Span span);

  /**
   * Set the ttl of a trace. Used to store a particular trace longer than the
   * default. It must be oh so interesting!
   */
  Future<Void> setTimeToLive(long traceId, long ttlSeconds);

  /**
   * Get the time to live for a specific trace.
   * If there are multiple ttl entries for one trace, pick the lowest one.
   */
  Future<Long> getTimeToLive(long traceId);

  Future<Set<Long>> tracesExist(Long... traceIds);

  /**
   * Get the available trace information from the storage system.
   * Spans in trace should be sorted by the first annotation timestamp
   * in that span. First event should be first in the spans list.
   *
   * The return list will contain only spans that have been found, thus
   * the return list may not match the provided list of ids.
   */
  Future<List<List<Span>>> getSpansByTraceIds(Long... traceIds);
  Future<List<Span>> getSpansByTraceId(long traceId);
  /**
   * How long do we store the data before we delete it? In seconds.
   */
  long getDataTimeToLive();

}
