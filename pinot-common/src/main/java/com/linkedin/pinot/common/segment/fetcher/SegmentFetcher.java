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

import com.linkedin.pinot.filesystem.PinotFS;
import com.linkedin.pinot.filesystem.PinotFSFactory;
import java.io.File;
import java.net.URI;
import java.util.Collections;
import java.util.Set;
import org.apache.commons.configuration.Configuration;


public class SegmentFetcher {
  private PinotFS _pinotFS;

  public void init(Configuration config) {
    _pinotFS.init(config);
  }

  public SegmentFetcher(Configuration config, URI uri) throws Exception {
    PinotFSFactory factory = new PinotFSFactory(config);
    _pinotFS = factory.create(uri);
  }

  public void fetchSegmentToLocal(URI uri, File tempFile) throws Exception {
    _pinotFS.copyToLocalFile(uri, tempFile.toURI());
  }

  /**
   * Returns a list of config keys whose value should not be logged.
   *
   * @return List of protected config keys
   */
  public Set<String> getProtectedConfigKeys() {
    return Collections.emptySet();
  }
}
