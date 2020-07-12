package com.dianping.squirrel.client.config.zookeeper;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;
import com.dianping.lion.Environment;
import com.dianping.squirrel.client.config.ClusterHotKeyConfig;
import com.dianping.squirrel.client.config.HotKeyConfig;
import com.dianping.squirrel.client.config.StoreCategoryConfigManager;
import com.dianping.squirrel.client.config.group.GroupAppMetaInfo;
import com.dianping.squirrel.client.config.group.GroupClusterConfig;
import com.dianping.squirrel.client.config.group.GroupClusterConfigUtil;
import com.dianping.squirrel.client.config.zookeeper.CacheEvent.CacheEventType;
import com.dianping.squirrel.client.core.StoreClientBuilder;
import com.dianping.squirrel.client.util.CuratorClientLionUtil;
import com.dianping.squirrel.client.util.IPUtils;
import com.dianping.squirrel.client.util.SquirrelManifest;
import com.dianping.squirrel.common.config.ConfigChangeListener;
import com.dianping.squirrel.common.config.ConfigManager;
import com.dianping.squirrel.common.config.ConfigManagerLoader;
import com.dianping.squirrel.common.domain.*;
import com.dianping.squirrel.common.exception.StoreConfigException;
import com.dianping.squirrel.common.util.JsonUtils;
import com.dianping.squirrel.common.util.NamedThreadFactory;
import com.dianping.squirrel.common.util.PathUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class CacheCuratorClient {
    //private static final String KEY_ZOOKEEPER_ADDRESS = "avatar-cache.zookeeper.address";
    private static final String KEY_ZOOKEEPER_RETRY_INTERVAL = "avatar-cache.zookeeper.retry.interval";
    private static final int DEFAULT_ZOOKEEPER_RETRY_INTERVAL = 1000;
    private static final String KEY_ZOOKEEPER_RETRY_LIMIT = "avatar-cache.zookeeper.retry.limit";
    private static final int DEFAULT_ZOOKEEPER_RETRY_LIMIT = 3;
    private static final String KEY_ZOOKEEPER_SYNC_INTERVAL = "avatar-cache.zookeeper.sync.interval";
    private static final long DEFAULT_ZOOKEEPER_SYNC_INTERVAL = 150 * 1000;
    private static final String KEY_ZOOKEEPER_FAIL_LIMIT = "avatar-cache.zookeeper.fail.limit";
    private static final int DEFAULT_ZOOKEEPER_FAIL_LIMIT = 300;
    private static final String WEB_CACHE = "web";
    private static Logger logger = LoggerFactory.getLogger(CacheCuratorClient.class);
    //private static CacheCuratorClient instance;
    private volatile String zkAddress;
    private PathProvider pathProvider;
    private volatile CuratorFramework curatorClient;
    private CacheMessageListener cacheMessageListener;
    private ExecutorService eventThreadPool;
    private ConfigManager configManager = ConfigManagerLoader.getConfigManager();
    private AtomicBoolean isSyncing = new AtomicBoolean(false);
    private volatile long lastSyncTime = System.currentTimeMillis();
    private volatile int retryInterval = configManager.getIntValue(KEY_ZOOKEEPER_RETRY_INTERVAL,
                                                                   DEFAULT_ZOOKEEPER_RETRY_INTERVAL);
    private volatile int retryLimit = configManager.getIntValue(KEY_ZOOKEEPER_RETRY_LIMIT,
                                                                DEFAULT_ZOOKEEPER_RETRY_LIMIT);

    // 300 次，结合定时任务，也相当于发生断连 300s 后才会 renew
    private volatile int failLimit = configManager.getIntValue(KEY_ZOOKEEPER_FAIL_LIMIT,
                                                               DEFAULT_ZOOKEEPER_FAIL_LIMIT);
    // 线上配置默认 300s 即 5分钟
    private volatile long syncInterval = configManager.getLongValue(KEY_ZOOKEEPER_SYNC_INTERVAL,
                                                                    DEFAULT_ZOOKEEPER_SYNC_INTERVAL);
    private int syncCount = 0;
    private int failCount = 0;

    private String lionZookeeperKey;
    private ConfigChangeListener configChangeListener;
    private AtomicBoolean closed = new AtomicBoolean(false);
    private AtomicBoolean exitSync = new AtomicBoolean(false);

    public CacheCuratorClient(String lionZookeeperKey) {
        try {
            this.lionZookeeperKey = lionZookeeperKey;
            init();
        } catch (Exception e) {
            logger.error("failed to init curator client", e);
        }
    }

    /**
     * 获取集群配置，并更新集群的最新修改时间到 CacheMessage ，以便后续的配置更新比较
     * @param clusterName 集群名称
     * @return 集群配置
     * @throws Exception zk 异常
     */
    public CacheConfigurationDTO getClusterConfig(String clusterName) throws Exception {
        if (isZookeeperConnected()) {
            String path = PathUtils.getClusterPath(clusterName);
            String content = getData(path, true);

            if (StringUtils.isBlank(content)) {
                logger.warn("cache service config [" + clusterName + "] is empty");

                return null;
            }
            CacheMessageManager.takeMessage(JsonUtils.fromStr(content, CacheConfigurationDTO.class));

            return JsonUtils.fromStr(content, CacheConfigurationDTO.class);
        }

        return null;
    }

    /**
     * 获取集群的地理位置信息， 这种一般是用于原先双中心改造过程中客户端配置的两个集群，
     * 没有集群组配置，所以需要额外的一个位置信息。 如果没有配置，客户端初始化会报错。
     * @param clusterName  集群名称
     * @return  集群的地理位置
     * @throws Exception zk 异常
     */
    public String getClusterRegion(String clusterName) throws Exception {
        String path = PathUtils.getClusterRegionPath(clusterName);
        String content = getData(path, false);

        if (StringUtils.isBlank(content)) {
            return null;
        }

        return content;
    }

    /**
     *  集群压测配置
     * @param clusterName 集群名称
     * @return 对应的压测配置
     */
    public ClusterRewriteConfig getClusterRewriteConfig(String clusterName) {
        if (isZookeeperConnected()) {
            try {
                String path = PathUtils.getClusterRewritePath(clusterName);
                String content = getData(path, true);

                if (StringUtils.isNotBlank(content)) {
                    return JsonUtils.fromStr(content, ClusterRewriteConfig.class);
                }

                return null;
            } catch (Exception e) {
                throw new StoreConfigException("cannot get cluster rewrite config from zookeeper.", e);
            }
        } else {
            throw new StoreConfigException("cannot get cluster rewrite config from zookeeper. ZookeeperConnected = false");
        }
    }

    /**
     * 集群组配置
     * @param groupName 集群组名称
     * @return  集群组配置
     */
    public GroupClusterConfig getGroupClusterConfig(String groupName) {
        if (isZookeeperConnected()) {
            try {
                String path = PathUtils.getGroupClusterPath(groupName);

                if (exists(path)) {
                    String content = getData(path, false);

                    if (StringUtils.isNotBlank(content)) {
                        return JsonUtils.fromStr(content, GroupClusterConfig.class);
                    }
                }

                return null;
            } catch (Exception e) {
                throw new StoreConfigException("cannot get cluster group config from zookeeper.", e);
            }
        } else {
            throw new StoreConfigException("cannot get cluster group config from zookeeper. ZookeeperConnected = false");
        }
    }

    /**
     * 集群到集群组的映射关系。 主要用于外卖侧业务不修改代码（还是使用集群名称的方式）直接在单元化机器上映射到集群组中对应的物理集群
     * @param clusterName 业务方原先使用的集群名称
     * @return 对应的集群组配置
     */
    public GroupAppMetaInfo getGroupMetaInfo(String clusterName) {
        if (isZookeeperConnected()) {
            try {
                String path = PathUtils.getGroupAppMetaInfoPath(clusterName);

                if (exists(path)) {
                    String content = getData(path, false);

                    if (StringUtils.isNotBlank(content)) {
                        return JsonUtils.fromStr(content, GroupAppMetaInfo.class);
                    }
                }

                return null;
            } catch (Exception e) {
                throw new StoreConfigException("cannot get cluster group metainfo from zookeeper.", e);
            }
        } else {
            throw new StoreConfigException("cannot get cluster group metainfo from zookeeper. ZookeeperConnected = false");
        }
    }

    /**
     * watch集群组配置动态更新通知节点
     * @param groupName
     * @return
     */
    public void watchGroupConfigUpdateState(String groupName) {
        if (isZookeeperConnected()) {
            try {
                String path = PathUtils.getGroupConfigUpdateStatePath(groupName);

                getData(path, true);
            } catch (Exception e) {
                throw new StoreConfigException("cannot get cluster group config update state from zookeeper.", e);
            }
        } else {
            throw new StoreConfigException("cannot get cluster group config update state from zookeeper. ZookeeperConnected = false");
        }
    }

    /**
     * 集群的热点Key配置
     * @param clusterName 集群名称
     * @return  热点Key配置
     */
    public Map<String, HotKeyConfig> getHotKeyConfig(String clusterName) {
        if (isZookeeperConnected()) {
            try {
                String path = PathUtils.getClusterHotKeyPath(clusterName);
                String content = getData(path, true);

                if (StringUtils.isNotBlank(content)) {
                    ClusterHotKeyConfig clusterHotKeyConfig = JsonUtils.fromStr(content, ClusterHotKeyConfig.class);
                    return clusterHotKeyConfig.getHotKeyConfigMap();
                }

                return new HashMap<String, HotKeyConfig>();
            } catch (Exception e) {
                throw new StoreConfigException("cannot get cluster hotKey config from zookeeper.", e);
            }
        } else {
            throw new StoreConfigException(
                    "cannot get cluster hotKey config from zookeeper. ZookeeperConnected = false");
        }
    }

    /**
     * 获取Category配置，如果在 新路径下不存在配置，则尝试从老的路径下寻找配置。
     * 注意：如果配置不存在，不要监听对应的路径
     * @param namespace namespace
     * @param category  Category
     * @return  Category 配置
     * @throws Exception zk 异常
     */
    public CategoryConfigurationDTO getCategoryConfig(String namespace, String category) throws Exception {
        if (isZookeeperConnected()) {
            if (!PathUtils.DEFAULT_NAMESPACE.equalsIgnoreCase(namespace)
                && exists(PathUtils.getCategoryPath(namespace, category))) {

                return getStrictCategoryConfig(namespace, category);
            } else {

                return getStrictCategoryConfig(PathUtils.DEFAULT_NAMESPACE, category);
            }
        } else {
            throw new StoreConfigException("ZookeeperConnected == false");
        }
    }

    /**
     * 严格按照传递的参数去寻找对应路径下的Category 配置；
     * 注意： CacheMessageManager.takeMessage() 是用来更新本地最新的Category的对应的更新时间。
     *        所以这里需要更新一下，下次拿到同样的配置对比下时间就不会去更新本地配置了
     * @param namespace namespace 配置
     * @param category Category 配置
     * @return category 配置
     * @throws Exception zk 异常
     */
    private CategoryConfigurationDTO getStrictCategoryConfig(String namespace, String category) throws Exception {
        if (isZookeeperConnected()) {
            // get category
            CategoryConfigurationDTO categoryConfig = _getCategoryConfig(namespace, category);
            if (categoryConfig == null) {
                return null;
            }
            categoryConfig.setNamespace(namespace);
            CacheKeyTypeVersionUpdateDTO versionChange = _getVersionChange(namespace, category);
            if (versionChange == null || "-1".equals(versionChange.getVersion())) {
                return null;
            }

            CacheMessageManager.takeMessage(versionChange);
            categoryConfig.setVersion(Integer.parseInt(versionChange.getVersion()));
            // get extension
            String extension = _getExtension(namespace, category);
            categoryConfig.setExtension(extension);
            // get flowControl
            String flowControl = _getFlowControl(namespace, category);
            categoryConfig.setFlowControl(flowControl);
            CacheMessageManager.takeMessage(categoryConfig);

            if (WEB_CACHE.equals(categoryConfig.getCacheType())) {
                String parentPath = PathUtils.getBatchKeyParentPath(category);
                cacheMessageListener.watchChildren(curatorClient, parentPath);
            }

            return categoryConfig;
        } else {
            throw new RuntimeException("ZookeeperConnected == false");
        }
    }

    private CategoryConfigurationDTO _getCategoryConfig(String namespace, String category) throws Exception {
        if (StringUtils.isNotBlank(namespace)) {
            String path = PathUtils.getCategoryPath(namespace, category);
            String content = getData(path, true);

            if (StringUtils.isNotBlank(content)) {
                return JsonUtils.fromStr(content, CategoryConfigurationDTO.class);
            }
        }

        return null;
    }

    private CacheKeyTypeVersionUpdateDTO _getVersionChange(String namespace, String category) throws Exception {
        if (StringUtils.isNotBlank(namespace)) {
            String path = PathUtils.getVersionPath(namespace, category);
            String content = getData(path, true);

            if (StringUtils.isNotBlank(content)) {
                return JsonUtils.fromStr(content, CacheKeyTypeVersionUpdateDTO.class);
            }
        }

        return null;
    }

    private String _getExtension(String namespace, String category) throws Exception {
        if (StringUtils.isNotBlank(namespace)) {
            String path = PathUtils.getExtensionPath(namespace, category);
            String content = getData(path, false);

            if (StringUtils.isNotBlank(content)) {
                return content;
            }
        }

        return null;
    }

    private String _getFlowControl(String namespace, String category) throws Exception {
        if (StringUtils.isNotBlank(namespace)) {
            String path = PathUtils.getFlowControlPath(namespace, category);
            String content = getData(path, false);

            if (StringUtils.isNotBlank(content)) {
                return content;
            }
        }

        return null;
    }

    /**
     * 获取当前应用之前使用过的 Category， 不过由于记录的不准确，后续需要废弃，替换成另外的方式
     * @param appName 应用名称
     * @return  对应的Category，格式   Category1，Category2，Category3
     * @throws Exception zk 异常
     */
    public String getRuntimeCategories(String appName) throws Exception {
        if (isZookeeperConnected() && StringUtils.isNotBlank(appName)) {
            String path = PathUtils.getRuntimeCategoryPath(appName);
            return getData(path, false);
        } else {
            return null;
        }
    }

    /**
     * 获取对应节点的内容
     * @param path  zk 目录
     * @param watch 是否需要监听，不管path 存不存在， 都可以监听
     * @return  对应目录下的内容
     * @throws Exception zk 异常
     */
    private String getData(String path, boolean watch) throws Exception {
        try {
            byte[] data;

            if (watch) {
                data = curatorClient.getData().watched().forPath(path);
            } else {
                data = curatorClient.getData().forPath(path);
            }

            return new String(data, "UTF-8");
        } catch (NoNodeException e) {
            if (watch) {
                curatorClient.checkExists().watched().forPath(path);
            }

            return null;
        }
    }

    /**
     * 更新对应zk节点的内容， 目前这里只用于本地缓存的删除通知
     * @param path  对应的zk节点
     * @param value value
     * @throws Exception zk 异常
     */
    protected void update(String path, String value) throws Exception {
        byte[] data = value.getBytes("UTF-8");

        if (curatorClient.checkExists().forPath(path) != null) {
            curatorClient.setData().forPath(path, data);
        } else {
            curatorClient.create().creatingParentsIfNeeded().forPath(path, data);
        }
    }

    /**
     * 更新临时节点， 目前只用于客户端初始化时候写入客户端版本号
     * @param path  临时节点的路径
     * @throws Exception zk 异常
     */
    private void updateTempNode(String path) throws Exception {
        byte[] data = "null".getBytes("UTF-8");

        if (curatorClient.checkExists().forPath(path) == null) {
            curatorClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, data);
        } else {
            curatorClient.setData().forPath(path, data);
        }
    }

    /**
     * 同步所有的 集群  以及  Category 配置。 有定时任务以及在zk发生重连后  两处在调用。
     * 最终是否更新本地的配置，则要看配置的修改时间是否改变（配置的addTime字段，非zk节点的修改时间），通过 takeMessage 判断
     * @param force 是否强制， 非强制则看是否在定时间隔内
     */
    private void syncAll(boolean force) {
        long now = System.currentTimeMillis();

        if ((force || (now - lastSyncTime >= syncInterval)) && !closed.get()) {
            if (isSyncing.compareAndSet(false, true)) {
                try {
                    _syncAll();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    Cat.logError("failed to sync store events for " + lionZookeeperKey, new StoreConfigException(e));
                } finally {
                    lastSyncTime = System.currentTimeMillis();
                    syncCount++;
                    isSyncing.set(false);
                }
            }
        }
    }

    private void _syncAll() throws Exception {
        if (isZookeeperConnected()) {
            Transaction t = Cat.newTransaction(CacheMessageListener.CAT_EVENT_TYPE, "sync.config");

            try {
                syncAllClusters();
                syncAllCategories();
                registerAllWatch();
                if (syncCount == 0) { // sync once
                    logClientVersion();
                }
                t.setStatus(Message.SUCCESS);
            } catch (Exception e) {
                t.setStatus(e);
                throw e;
            } finally {
                t.complete();
            }
        }
    }

    /**
     * 同步所有使用到的集群配置
     * @throws Exception zk 异常
     */
    private void syncAllClusters() throws Exception {
        Set<String> clusters = StoreClientBuilder.getCachedClients();
        Transaction t = Cat.getManager().getPeekTransaction();

        if (t != null) {
            t.addData("clusters", StringUtils.join(clusters, ','));
        }

        Set<String> myBgClusterNames = CuratorClientLionUtil.getMyBgClusters(lionZookeeperKey);
        for (String clusterName : clusters) {
            if (!myBgClusterNames.contains(clusterName)) {
                continue;
            }
            String path = PathUtils.getClusterPath(clusterName);
            String content = getData(path, true);

            if (StringUtils.isNotBlank(content)) {
                fireClusterChange(JsonUtils.fromStr(content, CacheConfigurationDTO.class));
            }
        }
    }

    /**
     * 同步所有使用的Category配置， 这里不同步老路径下的配置， 避免出现一些不可知的问题
     * @throws Exception zk 异常
     */
    private void syncAllCategories() throws Exception {
        Transaction t = Cat.getManager().getPeekTransaction();
        Set<String> usedCategories = StoreCategoryConfigManager.getUsedCategories();
        Set<String> namespaceSet = CacheMessageListener.getInstance().getRegisteredCategoryConfigListener().keySet();

        namespaceSet.remove(PathUtils.DEFAULT_NAMESPACE);

        if (t != null) {
            t.addData("categories", StringUtils.join(usedCategories, ','));
        }

        Set<String> myBgClusterNames = CuratorClientLionUtil.getMyBgClusters(lionZookeeperKey);
        for (String category : usedCategories) {
            for (String namespace : namespaceSet) {
                if (!myBgClusterNames.contains(namespace)) {
                    continue;
                }
                CategoryConfigurationDTO categoryConfig = _getCategoryConfig(namespace, category);

                if (categoryConfig == null) {
                    continue;
                }
                categoryConfig.setNamespace(namespace);
                CacheKeyTypeVersionUpdateDTO versionChange = _getVersionChange(namespace, category);

                if (versionChange == null) {
                    continue;
                }
                versionChange.setNamespace(namespace);
                fireVersionChange(versionChange);
                String extension = _getExtension(namespace, category);
                String flowControl = _getFlowControl(namespace, category);
                categoryConfig.setVersion(Integer.parseInt(versionChange.getVersion()));
                categoryConfig.setExtension(extension);
                categoryConfig.setFlowControl(flowControl);

                fireCategoryChange(categoryConfig);

                if (WEB_CACHE.equals(categoryConfig.getCacheType())) {
                    String parentPath = PathUtils.getBatchKeyParentPath(category);
                    cacheMessageListener.watchChildren(curatorClient, parentPath);
                }
            }
        }
    }

    /**
     * 重新注册所有的watch，以免监听失效
     */
    private void registerAllWatch() throws Exception {
        Set<String> clusters = StoreClientBuilder.getCachedClients();
        String appName = ConfigManagerLoader.getConfigManager().getAppName();
        Set<String> myBgClusterNames = CuratorClientLionUtil.getMyBgClusters(lionZookeeperKey);
        for (String clusterName : clusters) {
            if (!myBgClusterNames.contains(clusterName)) {
                continue;
            }
            // 压测配置监听
            getClusterRewriteConfig(clusterName);
            // 热点key配置监听
            getHotKeyConfig(clusterName);
            // 客户端配置动态变更监听
            if (StringUtils.isNotBlank(appName)) {
                getAppClientConfig(clusterName, appName);
            }
        }

        // 集群组配置变更监听
        Set<String> groups = CacheMessageListener.getInstance().queryRegisteredConfigUpdateGroup();
        Set<String> myBgGroupNames = CuratorClientLionUtil.getMyBgGroups(lionZookeeperKey);
        for (String groupName : groups) {
            if (StringUtils.isNotBlank(groupName) && myBgGroupNames.contains(groupName)) {
                GroupClusterConfigUtil.watchGroupConfigUpdateState(groupName);
            }
        }
    }

    private void logClientVersion() throws Exception {
        String app = Environment.getAppName() == null ? "defaultApplication" : Environment.getAppName();
        String localIp = IPUtils.getFirstNoLoopbackIP4Address();
        String path = PathUtils.getClientPath() + "/" + app + "/" + SquirrelManifest.getClientVersion() + "/" + localIp;

        updateTempNode(path);
    }

    private void fireClusterChange(CacheConfigurationDTO serviceConfig) {
        CacheEvent ce = new CacheEvent(CacheEventType.ServiceChange, serviceConfig);

        if (cacheMessageListener.dispatchCacheEvent(ce)) {
            Cat.logEvent(CacheMessageListener.CAT_EVENT_TYPE, "fireClusterChange", Message.SUCCESS,
                         serviceConfig.toString());
        }
    }

    private void fireCategoryChange(CategoryConfigurationDTO categoryConfig) {
        CacheEvent ce = new CacheEvent(CacheEventType.CategoryChange, categoryConfig);

        if (cacheMessageListener.dispatchCacheEvent(ce)) {
            Cat.logEvent(CacheMessageListener.CAT_EVENT_TYPE, "fireCategoryChange", Message.SUCCESS,
                         categoryConfig.toString());
        }
    }

    private void fireVersionChange(CacheKeyTypeVersionUpdateDTO versionChange) {
        CacheEvent ce = new CacheEvent(CacheEventType.VersionChange, versionChange);

        if (cacheMessageListener.dispatchCacheEvent(ce)) {
            Cat.logEvent(CacheMessageListener.CAT_EVENT_TYPE, "fireVersionChange", Message.SUCCESS,
                         versionChange.toString());
        }
    }

    private void init() throws Exception {
        zkAddress = configManager.getStringValue(lionZookeeperKey);
        pathProvider = new PathProvider();
        pathProvider.addTemplate("root", "/dp/cache/auth");
        pathProvider.addTemplate("cluster", "/dp/cache/auth/$0");
        pathProvider.addTemplate("applications", "/dp/cache/auth/$0/applications");

        if (StringUtils.isBlank(zkAddress)) {
            throw new NullPointerException("squirrel zookeeper address is empty");
        }
        configChangeListener = new ConfigChangeListener() {
            @Override
            public void onChange(String key, String value) {
                Cat.logEvent("SquirrelConfig", key + ":" + value);

                if (KEY_ZOOKEEPER_FAIL_LIMIT.equals(key)) {
                    failLimit = Integer.parseInt(value);
                } else if (KEY_ZOOKEEPER_RETRY_LIMIT.equals(key)) {
                    retryLimit = Integer.parseInt(value);
                } else if (KEY_ZOOKEEPER_RETRY_INTERVAL.equals(key)) {
                    retryInterval = Integer.parseInt(value);
                } else if (KEY_ZOOKEEPER_SYNC_INTERVAL.equals(key)) {
                    syncInterval = Long.parseLong(value);
                } else if (lionZookeeperKey.equals(key)) {
                    try {
                        // 随机sleep 一段时间，避免拉取风暴
                        Thread.sleep(new Random().nextInt(20 * 1000));
                    } catch (Exception ignore) {
                    } finally {
                        try {
                            if (StringUtils.isNotBlank(value)) {
                                zkAddress = value;
                                renewCuratorClient();
                            } else if (!CuratorClientLionUtil.DEFAULT_BG_ZK_KEY.equals(lionZookeeperKey)) {
                                // 改用中心集群
                                closeClientAndUseDefault();
                            }
                        } catch (Exception e) {
                            Cat.logError("renewCuratorClient error when zookeeper address changed: " + lionZookeeperKey, e);
                        }
                    }
                }
            }

        };
        configManager.registerConfigChangeListener(configChangeListener);

        eventThreadPool = new ThreadPoolExecutor(1, 4, 30L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(1000),
                                                 new NamedThreadFactory("squirrel-zookeeper-listen-pool", true), new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                logger.error("squirrel zk sync overflow!!!");
            }
        });

        cacheMessageListener = CacheMessageListener.getInstance();
        curatorClient = newCuratorClient();
        startEventSyncer();
    }

    private void startEventSyncer() {
        Thread t = new Thread(new CacheEventSyncer(), "squirrel-zookeeper-event-sync");

        t.setDaemon(true);
        t.start();
    }

    private void checkZookeeper() {
        boolean isConnected = false;

        try {
            isConnected = curatorClient.getZookeeperClient().getZooKeeper().getState().isConnected();
        } catch (Exception e) {
            logger.warn("failed to check zookeeper status: " + e.getMessage());
        }
        if (isConnected) {
            failCount = 0;
        } else {
            if (++failCount >= failLimit) {
                try {
                    renewCuratorClient();
                    failCount = 0;
                    logger.info("renewed curator client to " + zkAddress);
                    Cat.logEvent(CacheMessageListener.CAT_EVENT_TYPE, "zookeeper:renewSuccess:" + lionZookeeperKey, Message.SUCCESS, "" + failLimit);
                } catch (Exception e) {
                    failCount = failCount / 2;
                    logger.warn("failed to renew curator client: " + e.getMessage());
                    Cat.logEvent(CacheMessageListener.CAT_EVENT_TYPE, "zookeeper:renewFailure" + lionZookeeperKey, Message.SUCCESS, e.getMessage());
                }
            }
        }
    }

    private void renewCuratorClient() throws Exception {
        if (closed.get()) {
            return;
        }

        CuratorFramework newCuratorClient = newCuratorClient();
        CuratorFramework oldCuratorClient = this.curatorClient;

        this.curatorClient = newCuratorClient;
        syncAll(true);

        if (oldCuratorClient != null) {
            try {
                oldCuratorClient.close();
            } catch (Exception e) {
                logger.error("failed to close curator client: " + e.getMessage());
            }
        }
    }

    /**
     * 初始化一个zk客户端， 并注册相关的stateChanged
     * @return  zk 客户端
     * @throws Exception    zk 异常
     */
    private CuratorFramework newCuratorClient() throws Exception {
        CuratorFramework curatorClient = CuratorFrameworkFactory.newClient(zkAddress, 60 * 1000, 30 * 1000,
                                                                           new RetryNTimes(retryLimit, retryInterval));
        curatorClient.getConnectionStateListenable().addListener(new ConnectionStateListener() {

            @Override
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                logger.info(String.format("squirrel zookeeper %s state changed to %s", zkAddress, newState));
                Cat.logEvent(CacheMessageListener.CAT_EVENT_TYPE, "zookeeper:" + newState + ":" + lionZookeeperKey);

                // 在重连的时候同步配置。这里随机sleep一个时间，防止拉取风暴
                if (newState == ConnectionState.RECONNECTED) {
                    try {
                        Thread.sleep(new Random().nextInt(20 * 1000));
                    } catch (InterruptedException ignored) {
                        Cat.logError(ignored);
                    }
                    syncAll(true);
                }
            }

        }, eventThreadPool);
        curatorClient.getCuratorListenable().addListener(cacheMessageListener, eventThreadPool);
        curatorClient.start();

        if (!curatorClient.getZookeeperClient().blockUntilConnectedOrTimedOut()) {
            // if failed to connect to zookeeper, throw the exception out
            try {
                curatorClient.getZookeeperClient().getZooKeeper();
            } catch (Exception e) {
                // close the client to release resource
                curatorClient.close();
                throw e;
            }
        }

        return curatorClient;
    }

    private boolean exists(String path) throws Exception {
        Stat stat = curatorClient.checkExists().forPath(path);

        return stat != null;
    }

    /**
     * 当前集群是否严格鉴权
     * @param cluterName 集群名称
     * @return  是否严格鉴权
     */
    public boolean isStrict(String cluterName) {
        String value;

        try {
            value = getData(pathProvider.getPath("cluster", cluterName), false);
        } catch (Exception ignore) {
            return false;
        }

        return "true".equalsIgnoreCase(value);
    }

    /**
     * 获取被授权的应用，目前主要用在scan api 的授权。
     * @param clusterName 集群名称
     * @return 返回授权可以scan 当前集群的appkey列表
     */
    public Set<String> loadAuthedApps(String clusterName) {
        String data;

        try {
            data = getData(pathProvider.getPath("applications", clusterName), false);
        } catch (Exception ignore) {
            return null;
        }

        if (StringUtils.isNotBlank(data)) {
            String[] apps = data.split(",");
            Set<String> appSet = new HashSet<String>();

            for (String app : apps) {
                String trimedApp = app.trim();

                if (trimedApp.length() > 4) {
                    appSet.add(trimedApp);
                }
            }

            return appSet;
        }

        return Collections.EMPTY_SET;
    }

    /**
     * 定时任务。 1s 执行一次
     * 1，用于检查 zk 的链接情况， 如果链接在一段时间内异常（超过 failLimit 次异常）则重新创建客户端
     * 2，尝试调用syncAll 操作，同步配置
     */
    public AppClientConfigDTO getAppClientConfig(String clusterName, String appName) throws Exception {
        if (isZookeeperConnected()) {
            String path = PathUtils.getAppKeyPath(clusterName, appName);
            String content = getData(path, true);

            if (StringUtils.isNotBlank(content)) {

                return JsonUtils.fromStr(content, AppClientConfigDTO.class);
            }
        }

        return null;
    }

    class CacheEventSyncer implements Runnable {

        @Override
        public void run() {
            while (!Thread.interrupted() && !closed.get()) {
                try {
                    Thread.sleep(1000);
                    checkZookeeper();
                    syncAll(false);
                } catch (InterruptedException e) {
                    logger.warn("squirrel event sync thread is interrupted");
                    break;
                }
            }
            exitSync.set(true);
        }
    }

    public boolean isZookeeperConnected() {
        if (curatorClient == null) {
            logger.warn("curatorClient is null");
            return false;
        } else if (!curatorClient.getZookeeperClient().isConnected()) {
            logger.warn("zookeeper is not connected.");
            return false;
        }

        return true;
    }

    private void closeClientAndUseDefault() {
        if (closed.compareAndSet(false, true)) {
            while (!exitSync.get() || isSyncing.get()) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    logger.error("interrupted exception while waiting for exit: " + e.getMessage());
                }
            }

            if (curatorClient != null) {
                try {
                    curatorClient.close();
                } catch (Exception e) {
                    logger.error("failed to close curator client: " + e.getMessage());
                }
            }

            CuratorClientLionUtil.routeBgToDefault(lionZookeeperKey);
            while (CacheCuratorClientManager.getDefaultClient().isSyncing.get()) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    logger.error("interrupted exception while waiting for syncing: " + e.getMessage());
                }
            }
            CacheCuratorClientManager.getDefaultClient().syncAll(true);
            CacheCuratorClientManager.removeBgClient(lionZookeeperKey);
            configManager.unregisterConfigChangeListener(configChangeListener);

            Cat.logEvent("Squirrel.closeZk", lionZookeeperKey);
        }
    }
}
