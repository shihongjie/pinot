/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.routing;

import java.util.List;


/**
 * Routing table lookup request. Future filtering parameters for lookup needs to be added here.
 *
 *
 */
public class RoutingTableLookupRequest {

  private final String tableName;

  private final List<String> routingOptions;

  public String getTableName() {
    return tableName;
  }

  public List<String> getRoutingOptions() {
    return routingOptions;
  }

  public RoutingTableLookupRequest(String tableName, List<String> routingOptions) {
    super();
    this.tableName = tableName;
    this.routingOptions = routingOptions;
  }
}