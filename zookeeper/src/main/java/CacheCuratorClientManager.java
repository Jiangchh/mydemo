package com.dianping.squirrel.client.config.zookeeper;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CacheCuratorClientManager {
    private static final Map<String, com.dianping.squirrel.client.config.zookeeper.CacheCuratorClient> cacheCuratorClientMaps = new ConcurrentHashMap<>();

    public static com.dianping.squirrel.client.config.zookeeper.CacheCuratorClient getClusterClient(String clusterName) {
        String clusterZkKey = CuratorClientLionUtil.getClusterZkKey(clusterName);
        com.dianping.squirrel.client.config.zookeeper.CacheCuratorClient
                client = cacheCuratorClientMaps.get(clusterZkKey);
        if (client != null) {
            return client;
        }
        synchronized (cacheCuratorClientMaps) {
            client = cacheCuratorClientMaps.get(clusterZkKey);
            if (client != null) {
                return client;
            }
            client = new com.dianping.squirrel.client.config.zookeeper.CacheCuratorClient(clusterZkKey);
            cacheCuratorClientMaps.put(clusterZkKey, client);
            return client;
        }
    }

    public static com.dianping.squirrel.client.config.zookeeper.CacheCuratorClient getGroupClient(String groupName) {
        String groupZkKey = CuratorClientLionUtil.getGroupZkKey(groupName);
        com.dianping.squirrel.client.config.zookeeper.CacheCuratorClient client = cacheCuratorClientMaps.get(groupZkKey);
        if (client != null) {
            return client;
        }
        synchronized (cacheCuratorClientMaps) {
            client = cacheCuratorClientMaps.get(groupZkKey);
            if (client != null) {
                return client;
            }
            client = new com.dianping.squirrel.client.config.zookeeper.CacheCuratorClient(groupZkKey);
            cacheCuratorClientMaps.put(groupZkKey, client);
            return client;
        }
    }

    public static com.dianping.squirrel.client.config.zookeeper.CacheCuratorClient getDefaultClient() {
        String key = CuratorClientLionUtil.DEFAULT_BG_ZK_KEY;
        com.dianping.squirrel.client.config.zookeeper.CacheCuratorClient client = cacheCuratorClientMaps.get(key);
        if (client != null) {
            return client;
        }
        synchronized (cacheCuratorClientMaps) {
            client = cacheCuratorClientMaps.get(key);
            if (client != null) {
                return client;
            }
            client = new com.dianping.squirrel.client.config.zookeeper.CacheCuratorClient(key);
            cacheCuratorClientMaps.put(key, client);
            return client;
        }
    }

    public static void removeBgClient(String zkKey) {
        synchronized (cacheCuratorClientMaps) {
            cacheCuratorClientMaps.remove(zkKey);
        }
    }
}
