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

import java.io.IOException;
import java.util.*;

import com.ericsson.otp.erlang.*;
import com.yahoo.ycsb.*;
import org.apache.log4j.Logger;

/**
 * YCSB binding for
 * <a href="http://www.erlang.org">Distribtued Erlang</a>.
 *
 *
 *  > help
 Commands:
 read key [field1 field2 ...] - Read a record
 scan key recordcount [field1 field2 ...] - Scan starting at key
 insert key name1=value1 [name2=value2 ...] - Insert a new record
 update key name1=value1 [name2=value2 ...] - Update a record
 delete key - Delete a record
 table [tablename] - Get or [set] the name of the table
 quit - Quit
 *
 */
public class DistributedErlang extends DB {
  private static final Logger LOGGER = Logger.getLogger(DistributedErlang.class);

  private OtpSelf self = null;
  private OtpPeer node1 = null;
  private OtpConnection connection1 = null;

//  private OtpNode node1 = null;
//  private OtpNode node2 = null;
//  private OtpNode node3 = null;

  /**
   * Start distributed Erlang nodes.
   */
  @Override
  public void init() throws DBException {

    try {
      //hard coded path to the start_nodes script
      Process proc = Runtime.getRuntime().exec("/Users/bruce/git/YCSB/distributed-erlang/start_nodes");
      proc.waitFor();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    if(self == null) {
      try {
        self = new OtpSelf("ycsb@127.0.0.1");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    node1 = new OtpPeer("node1@127.0.0.1");

    if (connection1 == null) {
      try {
        connection1 = self.connect(node1);
      } catch (IOException e) {
        e.printStackTrace();
      } catch (OtpAuthException e) {
        e.printStackTrace();
      }
    } else {
      try {
        throw new IOException();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }



//    try {
//      node1 = new OtpNode("node1@127.0.0.1");
//      node2 = new OtpNode("node2@127.0.0.1");
//      node3 = new OtpNode("node3@127.0.0.1");
//
//      OtpMbox mbox1 = node1.createMbox();
//      OtpMbox mbox2 = node2.createMbox();
//      OtpMbox mbox3 = node3.createMbox();
//
//      OtpEpmd.publishPort(node1);
//      OtpEpmd.publishPort(node2);
//      OtpEpmd.publishPort(node3);
//
//    } catch (IOException e) {
//      e.printStackTrace();
//    }

  }

  @Override
  public void cleanup() throws DBException {
    connection1 = null;
    self = null;
//    try {
//      //hard coded path to the stop_nodes script
//      Process proc = Runtime.getRuntime().exec("/Users/bruce/git/YCSB/distributed-erlang/stop_nodes");
//      proc.waitFor();
//    } catch (IOException e) {
//      e.printStackTrace();
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
   *
   *       read key [field1 field2 ...] - Read a record
   *
   * @param table The name of the table
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return The result of the operation.
   */
  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {

    // global:re_register_name(Field, Value)

    HashMap<String, OtpErlangObject> queryResult = null;

    if (fields != null) {
      for (String field : fields) {
        System.out.println("field: " + field);
        OtpErlangObject resultPid = whereIsName(field);
        if (result != null) {
          queryResult.put(field, resultPid);
        } else {
          return Status.NOT_FOUND;
        }
      }
    }

    if (queryResult != null) {
      fillMap(result, queryResult);
    }
    return queryResult != null ? Status.OK : Status.NOT_FOUND;
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
    // global:re_register_name(Field, Value)

    for (Map.Entry entry : values.entrySet()) {
      System.out.println("key: " + entry.getKey() + " value: " + entry.getValue());
      Status result = reRegisterName(entry.getKey());

      if (result == Status.ERROR) {
        return Status.ERROR;
      }
    }

    return Status.OK;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written
   * into the record with the specified record key.
   *
   *       insert key name1=value1 [name2=value2 ...] - Insert a new record
   *
   * "For the write methods (insert() and update()) the methods take HashMap which maps field names to values."
   *
   *   > insert brianfrankcooper first=brian last=cooper
   Return code: 1
   191 ms
   > read brianfrankcooper
   Return code: 0
   last=cooper
   _id=brianfrankcooper
   first=brian
   2 ms
   > quit
   *
   * @param table The name of the table
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return The result of the operation.
   */
  @Override
  public Status insert(String table, String key,
                       HashMap<String, ByteIterator> values) {
    //goal: global:register_name(Field, Value)

    for (Map.Entry entry : values.entrySet()) {
      System.out.println("key: " + entry.getKey() + " value: " + entry.getValue());
      Status result = registerName(entry.getKey());

      if (result == Status.ERROR) {
        return Status.ERROR;
      }
    }

    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    return Status.OK;
  }


  private Status registerName(Object key) {
    try {
      /*
       * (bruce@127.0.0.1)6> proc_server:register_name('node1@127.0.0.1', asd).
       * yes
       */
      connection1.sendRPC(
          "proc_server",
          "register_name",
          new OtpErlangObject[]{
              new OtpErlangAtom("node1@127.0.0.1"),
              new OtpErlangAtom((String) key)
          }
      );
      return Status.OK;
    } catch (IOException e) {
      return Status.ERROR;
    }

//    OtpErlangAtom name = new OtpErlangAtom("name");
//    OtpErlangFun fun = null;
//    try {
//      fun = new OtpErlangFun();
//    } catch (OtpErlangDecodeException e) {
//      e.printStackTrace();
//    }
//    OtpErlangObject[] objects = new OtpErlangObject[]{name, fun};
//    OtpErlangList list = new OtpErlangList(objects);
//
//    try {
//      connection1.sendRPC("global","register_name", list);
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
  }

  private Status reRegisterName(Object key) {
    try {
      /*
       * (bruce@127.0.0.1)6> proc_server:re_register_name('node1@127.0.0.1', asd).
       * yes
       */
      connection1.sendRPC(
          "proc_server",
          "re_register_name",
          new OtpErlangObject[]{
              new OtpErlangAtom("node1@127.0.0.1"),
              new OtpErlangAtom((String) key)
          }
      );      return Status.OK;
    } catch (IOException e) {
      return Status.ERROR;
    }
  }


  private OtpErlangObject whereIsName(String field) {
    try {
        /*
         * (bruce@127.0.0.1)6> proc_server:whereis_name('node1@127.0.0.1', asd).
         * <0.76.0>
         */

        /*
          (node1@127.0.0.1)34> whereis(rex) ! {self(), { call, proc_server, whereis_name,
          ['node1@127.0.0.1', asd], user}}.
          {<0.107.0>,
           {call,proc_server,whereis_name,
                 ['node1@127.0.0.1',asd],
                 user}}
          (node1@127.0.0.1)35> flush().
          Shell got {rex,undefined}
          ok
         */
      connection1.sendRPC(
          "proc_server",
          "whereis_name",
          new OtpErlangObject[]{
              new OtpErlangAtom("node1@127.0.0.1"),
              new OtpErlangAtom(field)
          }
      );

      try {
        return connection1.receiveRPC();
      } catch (OtpErlangExit otpErlangExit) {
        otpErlangExit.printStackTrace();
        return null;
      } catch (OtpAuthException e) {
        e.printStackTrace();
        return null;
      }

    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  protected void fillMap(HashMap<String, ByteIterator> resultMap, Map<String, OtpErlangObject> queryResult) {
    for (Map.Entry<String, OtpErlangObject> entry : queryResult.entrySet()) {
      resultMap.put(
          entry.getKey(),
          new StringByteIterator(entry.getValue().toString())
      );
    }

  }
}
