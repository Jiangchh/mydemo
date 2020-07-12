package com.dianping.squirrel.client.config.zookeeper;

import com.dianping.cat.Cat;
import com.dianping.squirrel.client.config.*;
import com.dianping.squirrel.client.config.group.GroupClusterConfig;
import com.dianping.squirrel.client.config.group.GroupClusterConfigUtil;
import com.dianping.squirrel.client.config.listener.*;
import com.dianping.squirrel.client.config.zookeeper.CacheEvent.CacheEventType;
import com.dianping.squirrel.client.util.Constants;
import com.dianping.squirrel.common.domain.*;
import com.dianping.squirrel.common.util.JsonUtils;
import com.dianping.squirrel.common.util.PathUtils;
import com.dianping.squirrel.common.util.SedesUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CacheMessageListener implements CuratorListener {
    static final String CAT_EVENT_TYPE = "SquirrelConfig";
    private static final String CACHE_WEB_TYPE = "Squirrel.web";
    private static final String CACHE_SERVICE_PATH = "/dp/cache/service/";
    private static final String CACHE_CATEGORY_PATH = "/dp/cache/category/";
    private static final String CACHE_NAMESPACE_PATH = "/dp/cache/ns/";
    private static final String CACHE_GROUP_PATH = "/dp/cache/group/";
    private static final String CACHE_APPKEY_PATH = "/dp/cache/appkey";
    private static final String VERSION_SUFFIX = "/version";
    private static final String GROUP_CONFIG_UPDATE_SUFFIX = "/config-update";
    private static final String REWRITE_SUFFIX = "/rewrite-config";
    private static final String KEY_SUFFIX = "/key";
    private static final String BATCH_KEY = "/keys/";
    private final static String HOT_KEY_SUFFIX = "/hotKey-config";
    private static final Logger LOGGER = LoggerFactory.getLogger(CacheMessageListener.class);
    private static final CacheMessageListener INSTANCE = new CacheMessageListener();
    private final Map<String, List<StoreCategoryConfigListener>> categoryConfigListenerMap = new ConcurrentHashMap<String, List<StoreCategoryConfigListener>>();
    private CategoryVersionUpdateListener versionChangeListener = new CategoryVersionUpdateListener();
    private SingleCacheRemoveListener keyRemoveListener = new SingleCacheRemoveListener();
    private StoreClientConfigUpdateListener clientConfigUpdateListener = new StoreClientConfigUpdateListener();
    private CategoryConfigUpdateListener categoryConfigUpdateListener = new CategoryConfigUpdateListener();
    private HotKeyConfigUpdateListener hotKeyConfigUpdateListener = new HotKeyConfigUpdateListener();
    private AppClientConfigChangeListener appClientConfigChangeListener = new AppClientConfigChangeListener();
    private GroupConfigUpdateListener groupConfigUpdateListener = new GroupConfigUpdateListener();
    private ConcurrentMap<String, List<String>> pathChildrenMap = new ConcurrentHashMap<String, List<String>>();

    private CacheMessageListener() {
    }

    public static CacheMessageListener getInstance() {
        return INSTANCE;
    }

    @Override
    public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
        if (event == null) {
            LOGGER.warn("curator event is null");
            return;
        }

        if (CuratorEventType.WATCHED == event.getType()) {
            WatchedEvent we = event.getWatchedEvent();

            if (we == null) {
                LOGGER.warn("zookeeper event is null");
                return;
            }

            if (we.getType() == EventType.None) {
                return;
            }
            String path = we.getPath();

            try {
                if (we.getType() == EventType.NodeDataChanged || we.getType() == EventType.NodeCreated) {
                    processDataChanged(client, path);
                } else if (we.getType() == EventType.NodeChildrenChanged) {
                    // cache server added, removed or ip address changed will trigger this event
                    processChildrenChanged(client, path);
                } else if (we.getType() == EventType.NodeDeleted) {
                    // we.getType() == EventType.NodeDeleted
//                    processNodeDeleted(client, path);
                } else {
                    watch(client, path);
                }
            } catch (Exception e) {
                LOGGER.error("error in cache message listener, path: " + path, e);
            }
        }
    }

    void watchChildren(CuratorFramework curatorClient, String parentPath) throws Exception {
        if (!pathChildrenMap.containsKey(parentPath)) {
            ZKPaths.mkdirs(curatorClient.getZookeeperClient().getZooKeeper(), parentPath);
            List<String> children = curatorClient.getChildren().watched().forPath(parentPath);

            if (children != null) {
                pathChildrenMap.put(parentPath, children);
            }

            if (children != null && children.size() > 0) {
                for (String child : children) {
                    watch(curatorClient, parentPath + "/" + child);
                }
            }
        }
    }

    private void processChildrenChanged(CuratorFramework client, String path) throws Exception {
        List<String> children;

        try {
            children = client.getChildren().watched().forPath(path);
        } catch (NoNodeException e) {
            ZKPaths.mkdirs(client.getZookeeperClient().getZooKeeper(), path);
            children = client.getChildren().watched().forPath(path);
        }

        if (children == null) {
            return;
        }
        List<String> oldChildren = pathChildrenMap.get(path);

        pathChildrenMap.put(path, children);

        if (oldChildren != null) {
            children.removeAll(oldChildren);
        }

        for (String child : children) {
            String childPath = path + "/" + child;

            processDataChanged(client, childPath);
        }
    }

    private void processDataChanged(CuratorFramework client, String path) throws Exception {
        String content = getData(client, path, true);
        LOGGER.info(String.format("received store notification, path: %s, content: %s", path, content));

        if (content == null) {
            return;
        }
        com.dianping.squirrel.client.config.zookeeper.CacheEvent ce = parseEvent(path, content);

        if (ce == null) {
            LOGGER.error(String.format("failed to parse store event, path: %s, content: %s", path, content));
            return;
        }

        if (ce.getContent() instanceof CategoryConfigurationDTO) {
            // get flowControl
            CategoryConfigurationDTO categoryConfig = (CategoryConfigurationDTO) ce.getContent();
            String flowControl = getData(client, PathUtils.getFlowControlPath(categoryConfig.getNamespace(), categoryConfig.getCategory()), false);
            if (flowControl == null) {
                flowControl = getData(client, PathUtils.getFlowControlPath(categoryConfig.getCategory()), false);
            }
            categoryConfig.setFlowControl(flowControl);

            //get extension
            String extension = getData(client, PathUtils.getExtensionPath(categoryConfig.getNamespace(), categoryConfig.getCategory()), false);
            if (extension == null) {
                extension = getData(client, PathUtils.getExtensionPath(categoryConfig.getCategory()), false);
            }
            categoryConfig.setExtension(extension);
        }
        dispatchCacheEvent(ce);
    }

    private com.dianping.squirrel.client.config.zookeeper.CacheEvent parseEvent(String path, String content) throws Exception {
        com.dianping.squirrel.client.config.zookeeper.CacheEvent ce = null;

        if (path.startsWith(CACHE_CATEGORY_PATH)) {
            ce = parseCategoryEvent(path, content);
        } else if (path.startsWith(CACHE_NAMESPACE_PATH)) {
            ce = parseNamespaceCategoryEvent(path, content);
        } else if (path.startsWith(CACHE_SERVICE_PATH)) {
            ce = parseServiceEvent(path, content);
        } else if (path.startsWith(CACHE_GROUP_PATH)) {
            ce = parseGroupEvent(path, content);
        } else if (path.startsWith(CACHE_APPKEY_PATH)) {
            ce = parseAppKeyEvent(path, content);
        }

        return ce;
    }

    private com.dianping.squirrel.client.config.zookeeper.CacheEvent parseCategoryEvent(String path, String content) throws IOException {
        com.dianping.squirrel.client.config.zookeeper.CacheEvent
                ce = new com.dianping.squirrel.client.config.zookeeper.CacheEvent();

        if (path.endsWith(VERSION_SUFFIX)) {
            CacheKeyTypeVersionUpdateDTO dto = JsonUtils.fromStr(content, CacheKeyTypeVersionUpdateDTO.class);

            ce.setType(CacheEventType.VersionChange);
            ce.setContent(dto);
        } else if (path.endsWith(KEY_SUFFIX)) {
            ce.setType(CacheEventType.KeyRemove);
            ce.setContent(JsonUtils.fromStr(content, SingleCacheRemoveDTO.class));
        } else if (path.contains(BATCH_KEY)) {
            ce.setType(CacheEventType.BatchKeyRemove);
            ce.setContent(SedesUtils.deserialize(content));
        } else {
            CategoryConfigurationDTO dto = JsonUtils.fromStr(content, CategoryConfigurationDTO.class);

            dto.setNamespace(dto.getCacheType());
            ce.setType(CacheEventType.CategoryChange);
            ce.setContent(dto);
        }

        return ce;
    }

    private com.dianping.squirrel.client.config.zookeeper.CacheEvent parseNamespaceCategoryEvent(String path, String content) throws IOException {
        com.dianping.squirrel.client.config.zookeeper.CacheEvent
                ce = new com.dianping.squirrel.client.config.zookeeper.CacheEvent();
        String namespace = getNamespaceFromPath(path);

        if (path.endsWith(VERSION_SUFFIX)) {
            ce.setType(CacheEventType.VersionChange);
            CacheKeyTypeVersionUpdateDTO dto = JsonUtils.fromStr(content, CacheKeyTypeVersionUpdateDTO.class);

            dto.setNamespace(namespace);
            ce.setContent(dto);
        } else {
            ce.setType(CacheEventType.CategoryChange);
            CategoryConfigurationDTO dto = JsonUtils.fromStr(content, CategoryConfigurationDTO.class);

            dto.setNamespace(namespace);
            ce.setContent(dto);
        }

        return ce;
    }

    private com.dianping.squirrel.client.config.zookeeper.CacheEvent parseServiceEvent(String path, String content) throws IOException {
        com.dianping.squirrel.client.config.zookeeper.CacheEvent
                ce = new com.dianping.squirrel.client.config.zookeeper.CacheEvent();

        if (StringUtils.countMatches(path, "/") == 4) { // 集群IP绝对路径
            ce.setType(CacheEventType.ServiceChange);
            ce.setContent(JsonUtils.fromStr(content, CacheConfigurationDTO.class));
        } else {
            if (path.endsWith(REWRITE_SUFFIX)) {
                ce.setType(CacheEventType.ServiceRewriteChange);
                ce.setContent(JsonUtils.fromStr(content, ClusterRewriteConfig.class));
            } else if (path.endsWith(HOT_KEY_SUFFIX)) {
                ce.setType(CacheEventType.HotKeyChange);
                ce.setContent(JsonUtils.fromStr(content, ClusterHotKeyConfig.class));
            } else {
                return null;
            }
        }

        return ce;
    }

    private com.dianping.squirrel.client.config.zookeeper.CacheEvent parseAppKeyEvent(String path, String content) throws IOException {
        com.dianping.squirrel.client.config.zookeeper.CacheEvent
                ce = new com.dianping.squirrel.client.config.zookeeper.CacheEvent();
        ce.setType(CacheEventType.AppClientConfigChange);
        ce.setContent(JsonUtils.fromStr(content, AppClientConfigDTO.class));

        return ce;
    }

    private com.dianping.squirrel.client.config.zookeeper.CacheEvent parseGroupEvent(String path, String content) throws IOException {
        com.dianping.squirrel.client.config.zookeeper.CacheEvent ce = null;

        if (path.endsWith(GROUP_CONFIG_UPDATE_SUFFIX)) {
            String groupName = getGroupNameFromPath(path);
            GroupClusterConfig groupClusterConfig = null;
            try {
                groupClusterConfig = GroupClusterConfigUtil.getGroupClusterConfig(groupName);
            } catch (Exception ignore) {
            }
            ce = new com.dianping.squirrel.client.config.zookeeper.CacheEvent();
            ce.setType(CacheEventType.GroupConfigChange);
            ce.setContent(groupClusterConfig);
        }

        return ce;
    }

    @SuppressWarnings("unchecked")
    public synchronized boolean dispatchCacheEvent(com.dianping.squirrel.client.config.zookeeper.CacheEvent ce) {
        switch (ce.getType()) {
        case VersionChange:
            CacheKeyTypeVersionUpdateDTO versionChange = (CacheKeyTypeVersionUpdateDTO) ce.getContent();

            if (CacheMessageManager.takeMessage(versionChange)) {
                Cat.logEvent(CAT_EVENT_TYPE, "clear.category:" + versionChange.getMsgValue(), "0", versionChange.getVersion());
                versionChangeListener.handleMessage(versionChange);

                return true;
            }

            return false;
        case KeyRemove:
            SingleCacheRemoveDTO keyRemove = (SingleCacheRemoveDTO) ce.getContent();

            Cat.logEvent(CACHE_WEB_TYPE, "clear.key:" + PathUtils.getCategoryFromKey(keyRemove.getCacheKey()));
            keyRemoveListener.handleMessage(keyRemove);

            return true;
        case BatchKeyRemove:
            List<SingleCacheRemoveDTO> keyRemoves = (List<SingleCacheRemoveDTO>) ce.getContent();

            if (keyRemoves == null || keyRemoves.size() == 0) {
                return false;
            }
            String category = PathUtils.getCategoryFromKey(keyRemoves.get(0).getCacheKey());

            for (SingleCacheRemoveDTO singleKeyRemove : keyRemoves) {
                Cat.logEvent(CACHE_WEB_TYPE, "clear.key:" + category);
                keyRemoveListener.handleMessage(singleKeyRemove);
            }

            return true;
        case CategoryChange:
            CategoryConfigurationDTO categoryChange = (CategoryConfigurationDTO) ce.getContent();

            if (CacheMessageManager.takeMessage(categoryChange)) {
                Cat.logEvent(CAT_EVENT_TYPE, "category.change:" + categoryChange.getCategory(), "0", "" + ce.getContent());
                categoryConfigUpdateListener.handleMessage(categoryChange);

                return true;
            }

            return false;
        case ServiceChange:
            CacheConfigurationDTO serviceChange = (CacheConfigurationDTO) ce.getContent();

            if (CacheMessageManager.takeMessage(serviceChange)) {
                Cat.logEvent(CAT_EVENT_TYPE, "service.change:" + serviceChange.getCacheKey(), "0", "" + ce.getContent());
                clientConfigUpdateListener.handleMessage(serviceChange);

                return true;
            }

            return false;
        case ServiceRewriteChange:
            ClusterRewriteConfig serviceSettingChange = (ClusterRewriteConfig) ce.getContent();

            clientConfigUpdateListener.handleMessage(serviceSettingChange);
            Cat.logEvent(CAT_EVENT_TYPE, "service.rewrite.change:" + serviceSettingChange.getCluster(), "0", "" + ce.getContent());

            return true;
        case HotKeyChange:
            ClusterHotKeyConfig clusterHotKeyConfig = (ClusterHotKeyConfig) ce.getContent();
            Cat.logEvent(CAT_EVENT_TYPE, "hotkey.change" + clusterHotKeyConfig.getClusterName(), "0", "" + ce.getContent());
            hotKeyConfigUpdateListener.handleMessage(clusterHotKeyConfig);

            return true;
        case GroupConfigChange:
            if (ce.getContent() == null) {
                Cat.logEvent(CAT_EVENT_TYPE, "groupConfig.change.error", "0", "empty group config");
                return false;
            }
            GroupClusterConfig groupClusterConfig = (GroupClusterConfig) ce.getContent();
            Cat.logEvent(CAT_EVENT_TYPE, "groupConfig.change" + groupClusterConfig.getGroupName(), "0", "" + ce.getContent());
            groupConfigUpdateListener.handleMessage(groupClusterConfig);

            return true;
        case AppClientConfigChange:
            AppClientConfigDTO appClientConfigChange = (AppClientConfigDTO) ce.getContent();
            Cat.logEvent(CAT_EVENT_TYPE, "appClientConfig.change" + appClientConfigChange.getClusterName(), "0", "" + ce.getContent());
            appClientConfigChangeListener.handleMessage(appClientConfigChange);

            return true;
        default:
            LOGGER.error("invalid store event" + ce.getType());

            return false;
        }
    }

    private String getData(CuratorFramework client, String path, boolean watch) throws Exception {
        try {
            byte[] data;

            if (watch) {
                data = client.getData().watched().forPath(path);
            } else {
                data = client.getData().forPath(path);
            }

            return new String(data, "UTF-8");
        } catch (NoNodeException e) {
            if (watch) {
                client.checkExists().watched().forPath(path);
            }

            return null;
        }
    }

    private void watch(CuratorFramework client, String path) throws Exception {
        client.checkExists().watched().forPath(path);
    }

    private String getNamespaceFromPath(String path) {
        int start = CACHE_NAMESPACE_PATH.length();
        int end = path.indexOf("/", start + 1);

        return path.substring(start, end);
    }

    private String getGroupNameFromPath(String path) {
        int start = CACHE_GROUP_PATH.length();
        int end = path.indexOf("/", start + 1);

        return path.substring(start, end);
    }

    public void addStoreClientConfigListener(String storeType, StoreClientConfigListener listener) {
        clientConfigUpdateListener.addConfigListener(storeType, listener);
    }

    public void addCategoryConfigListener(String namespace, StoreCategoryConfigListener listener) {
        synchronized (categoryConfigListenerMap) {
            List<StoreCategoryConfigListener> listeners = categoryConfigListenerMap.get(namespace);

            if (listeners == null) {
                listeners = new ArrayList<StoreCategoryConfigListener>();
                categoryConfigListenerMap.put(namespace, listeners);
                //因为业务端注册listerer都是根据代码配置的集群名称的，所以要添加redis-cache-ehcache默认listener
                categoryConfigListenerMap.put(Constants.DEFAULT_EHCACHE_CLUSTER, listeners);
            }
            listeners.add(listener);
        }
    }

    public void addGroupConfigListener(String groupName, GroupConfigListener listener) {
        groupConfigUpdateListener.addListener(groupName, listener);
    }

    public Set<String> queryRegisteredConfigUpdateGroup() {
        return groupConfigUpdateListener.queryGroupName();
    }

    public void removeCategoryConfigListener(String namespace, StoreCategoryConfigListener listener) {
        synchronized (categoryConfigListenerMap) {
            List<StoreCategoryConfigListener> listeners = categoryConfigListenerMap.get(namespace);

            if (listeners != null) {
                listeners.remove(listener);
            }
        }
    }

    public Map<String, List<StoreCategoryConfigListener>> getRegisteredCategoryConfigListener() {
        return categoryConfigListenerMap;
    }

    public void addHotKeyConfigListener(String clusterName, HotKeyConfigListener hotKeyConfigListener) {
        hotKeyConfigUpdateListener.addConfigListener(clusterName, hotKeyConfigListener);
    }

    public void addAppClientConfigListener(String clusterName, AppClientConfigListener appClientConfigListener) {
        appClientConfigChangeListener.addConfigListener(clusterName, appClientConfigListener);
    }
}
