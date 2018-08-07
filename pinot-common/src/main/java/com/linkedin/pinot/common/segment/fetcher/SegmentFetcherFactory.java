/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.segment.fetcher;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentFetcherFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentFetcherFactory.class);
  private static final SegmentFetcherFactory INSTANCE = new SegmentFetcherFactory();

  private SegmentFetcherFactory() {
  }

  public static SegmentFetcherFactory getInstance() {
    return INSTANCE;
  }

  private final Map<String, SegmentFetcher> _segmentFetcherMap = new HashMap<>();

  /**
   * Initiate the segment fetcher factory. This method should only be called once.
   *
   * @param config Segment fetcher factory config
   */
  public void init(Configuration config) throws ClassNotFoundException, IllegalAccessException, InstantiationException {}

  public boolean containsProtocol(String protocol) {
    return _segmentFetcherMap.containsKey(protocol);
  }

  public SegmentFetcher getSegmentFetcherBasedOnURI(String uri) throws URISyntaxException {
    String protocol = new URI(uri).getScheme();
    return _segmentFetcherMap.get(protocol);
  }
}
