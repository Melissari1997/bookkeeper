/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package testisw2.util;

import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;
import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.test.ClientBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the zookeeper utilities.
 */
public class ZooKeeperSetup{

    static {
        // org.apache.zookeeper.test.ClientBase uses FourLetterWordMain, from 3.5.3 four letter words
        // are disabled by default due to security reasons
        System.setProperty("zookeeper.4lw.commands.whitelist", "*");
    }
    static final Logger LOG = LoggerFactory.getLogger(ZooKeeperSetup.class);

    // ZooKeeper related variables
    protected Integer zooKeeperPort = 0;
    private InetSocketAddress zkaddr;

    protected ZooKeeperServer zks;
    protected ZooKeeper zkc; // zookeeper client
    protected NIOServerCnxnFactory serverFactory;
    protected File zkTmpDir;
    private String connectString;

    public ZooKeeperSetup() {
        String loopbackIPAddr = InetAddress.getLoopbackAddress().getHostAddress();
        zkaddr = new InetSocketAddress(loopbackIPAddr, 0);
        connectString = loopbackIPAddr + ":" + zooKeeperPort;
    }

    public ZooKeeper getZooKeeperClient() {
        return zkc;
    }


    public String getMetadataServiceUri(String zkLedgersRootPath) {
        return "zk://" + connectString + zkLedgersRootPath;
    }

    public void startZookeeper() throws Exception {
        // create a ZooKeeper server(dataDir, dataLogDir, port)
        LOG.debug("Running ZK server");
        zkTmpDir = IOUtils.createTempDir("zookeeper", "test");

        // start the server and client.
        zks = new ZooKeeperServer(zkTmpDir, zkTmpDir,
                ZooKeeperServer.DEFAULT_TICK_TIME);
        serverFactory = new NIOServerCnxnFactory();
        serverFactory.configure(zkaddr, 3);
        serverFactory.startup(zks);

        if (0 == zooKeeperPort) {
            zooKeeperPort = serverFactory.getLocalPort();
            zkaddr = new InetSocketAddress(zkaddr.getHostName(), zooKeeperPort);
            connectString = zkaddr.getHostName() + ":" + zooKeeperPort;
        }

        boolean b = ClientBase.waitForServerUp(connectString,
                ClientBase.CONNECTION_TIMEOUT);
        LOG.debug("Server up: " + b);

        // create a zookeeper client
        LOG.debug("Instantiate ZK Client");
        zkc = ZooKeeperClient.newBuilder()
                .connectString(connectString)
                .sessionTimeoutMs(10000)
                .build();

        // create default bk ensemble
        createBKEnsemble("/ledgers");
    }
    
    void createBKEnsemble(String ledgersPath) throws KeeperException, InterruptedException {
        Transaction txn = getZooKeeperClient().transaction();
        txn.create(ledgersPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        txn.create(ledgersPath + "/" + AVAILABLE_NODE, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        txn.create(ledgersPath + "/" + AVAILABLE_NODE + "/" + READONLY, new byte[0], Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        txn.commit();
    }
}
