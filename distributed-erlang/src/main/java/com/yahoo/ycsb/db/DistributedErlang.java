/**
 * Copyright (c) 2016 Bruce Yinhe. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import java.lang.Override;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

/**
 * YCSB binding for
 * <a href="http://www.project-voldemort.com/voldemort/">Voldemort</a>.
 */
public class DistributedErlang extends DB {
  private static final Logger LOGGER = Logger.getLogger(VoldemortClient.class);

  private String storeName;

  /**
   * Initialize the DB layer. This accepts all properties allowed by the
   * Voldemort client. A store maps to a table. Required : bootstrap_urls
   * Additional property : store_name -> to preload once, should be same as -t
   * {@link ClientConfig}
   */
  @Override
  public void init() throws DBException {

  }

  @Override
  public void cleanup() throws DBException {

  }

  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String,String> result) {

  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    LOGGER.warn("Distributed Erlang does not support Scan semantics");
    return Status.OK;
  }

  @Override
  public Status update(String table, String key,
                       HashMap<String, ByteIterator> values) {

  }

  @Override
  public Status insert(String table, String key,
                       HashMap<String, ByteIterator> values) {

  }

  @Override
  public Status delete(String table, String key) {

  }

}
