package com.simplekafka.broker;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.w3c.dom.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;
import java.util.logging.Level;

public class ZookeeperClient implements Watcher  {

    private static final Logger LOGGER = Logger.getLogger(ZookeeperClient.class.getName());
    private final static int SESSION_TIMEOUT = 30000;

    private final String host;
    private final int port;
    private ZooKeeper zooKeeper;
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    public ZookeeperClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    // Connect to a Zookeeper
    public void connect() throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(host, SESSION_TIMEOUT, this);
        connectedSignal.await();

        createPath("/brokers");
        createPath("/topics");
        createPath("/controllers");
    }

    // Gets a connection string
    public String getConnectionString() {
        return host + ":" + port;
    }

    // Close conn
    public void close() throws InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }

    // Create a persistent node
    public void createPersistentNode(String path, String data) throws KeeperException, InterruptedException {
        Stat stat =  zooKeeper.exists(path,false);
        if (stat == null) {
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            LOGGER.info(String.format("Created persistent node: %s", path));
        } else {
            zooKeeper.setData(path, data.getBytes(), -1);
            LOGGER.info("Updated persistent node: " + path);
        }
    }

    // Create an ephemeral node
    public boolean createEphemeralNode(String path, String data) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            LOGGER.info(String.format("Created ephemeral node: %s", path));
            return true;
        } else {
            LOGGER.info("Ephemeral node already exists: " + path);
            return false;
        }
    }

    // Check if path exists
    public boolean exists(String path) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path,false);
        return stat != null;
    }

    // Get data from a node
    public String getData(String path) throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData(path, false, null);
        return new String(data);
    }

    // Set data for a node
    public void setData(String path, String data) throws KeeperException, InterruptedException {
        zooKeeper.setData(path, data.getBytes(), -1);
    }

    // Get children of a path
    public List<String> getChildren(String path) throws KeeperException, InterruptedException {
        try {
            return zooKeeper.getChildren(path, false);
        } catch (KeeperException.NoNodeException e) {
            return new ArrayList<>();
        }
    }

    // Create a path recursively
    private void createPath(String path) {
        try {
            if (path.equals("/")) {
                return;
            }

            int lastSlashIndex = path.lastIndexOf("/");
            if (lastSlashIndex > 0) {
                String parentPath = path.substring(0, lastSlashIndex);
                createPath(parentPath);
            }

            if (zooKeeper.exists(path, false) == null) {
                zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                LOGGER.info("Created ZooKeeper path: " + path);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to create path: " + path, e);
        }
    }

    // Watch for children changes
    public void watchChildren(String path, ChildrenCallback callback) {
        try {
            List<String> children = zooKeeper.getChildren(path, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    try {
                        List<String> newChildren = zooKeeper.getChildren(path, event2 -> {
                            if (event2.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                                watchChildren(path, callback);
                            }
                        });
                        callback.onChildrenChanged(newChildren);
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Error processing children changed event", e);
                    }
                }
            });
            callback.onChildrenChanged(children);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch children for path: " + path, e);
        }
    }

    // Watch for node changes
    public void watchNode(String path, NodeCallBack callBack) {
        try {
            zooKeeper.exists(path, event -> {
                if (event.getType() == Event.EventType.NodeDeleted) {
                    callBack.onNodeChanged();
                } else if (event.getType() == Event.EventType.NodeDataChanged) {
                    callBack.onNodeChanged();
                } else if  (event.getType() == Event.EventType.NodeCreated) {
                    callBack.onNodeChanged();
                }
            });
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to watch node: " + path, e);
        }
    }

    // Delete a node
    public void deleteNode(String path) throws KeeperException, InterruptedException {
        if (exists(path)) {
            zooKeeper.delete(path, -1);
            LOGGER.info("Deleted ZooKeeper path: " + path);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
            LOGGER.info("ZooKeeper connected");
        } else if (event.getState() == Event.KeeperState.Disconnected) {
            LOGGER.info("Disconnected from ZooKeeper");
        } else if (event.getState() == Event.KeeperState.Expired) {
            LOGGER.warning("ZooKeeper session expired, reconnecting...");
            try {
                if (zooKeeper != null) {
                    zooKeeper.close();
                }
                connectedSignal = new CountDownLatch(1);
                zooKeeper = new ZooKeeper(getConnectionString(), SESSION_TIMEOUT, this);
                connectedSignal.await();
                LOGGER.info("Reconnected to ZooKeeper after session expiry");
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Failed to reconnect to ZooKeeper", e);
            }
        }
    }

    public interface ChildrenCallback {
        void onChildrenChanged(List<String> children);
    }

    public interface NodeCallBack {
        void onNodeChanged();
    }
}
