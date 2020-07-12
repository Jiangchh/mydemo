package com.dianping.squirrel.client.config.zookeeper;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CacheMessageNotifier {
    private static Logger logger = LoggerFactory.getLogger(CacheMessageNotifier.class);
    private static final long DEFAULT_BATCH_REMOVE_INTERVAL = 1500;
    private static final int DEFAULT_MAX_KEYS_PER_CATEGORY = 5000;

    private long keyRemoveInterval;
    private Map<String, List<SingleCacheRemoveDTO>> keyRemoveBuffer;
    private int maxKeysPerCategory;
    private long lastKeyRemoveTime = System.currentTimeMillis();
    private static CacheMessageNotifier INSTANCE = null;

    private CacheMessageNotifier() {
        keyRemoveInterval =  DEFAULT_BATCH_REMOVE_INTERVAL;
        keyRemoveBuffer = new HashMap<String, List<SingleCacheRemoveDTO>>();
        maxKeysPerCategory = DEFAULT_MAX_KEYS_PER_CATEGORY;

        Thread t = new Thread("cache-batch-key-remove-thread") {
            public void run() {
                doBatchKeyRemove();
            }
        };
        t.start();
    }

    public static CacheMessageNotifier getInstance() {
        if (INSTANCE == null) {
            synchronized (CacheMessageNotifier.class) {
                if (INSTANCE == null) {
                    INSTANCE = new CacheMessageNotifier();
                }
            }
        }

        return INSTANCE;
    }

    @SuppressWarnings("serial")
    public void addToKeyRemoveBuffer(SingleCacheRemoveDTO message) {
        String category = PathUtils.getCategoryFromKey(message.getCacheKey());
        if(category != null) {
            synchronized(this) {
                List<SingleCacheRemoveDTO> list = keyRemoveBuffer.get(category);
                if(list == null) {
                    list = new BoundedLinkedList<SingleCacheRemoveDTO>(maxKeysPerCategory) {
                        protected void overflow(SingleCacheRemoveDTO keyRemove) {
                            logger.error("key remove event overflow! drop event: " + keyRemove);
                        }
                    };
                    keyRemoveBuffer.put(category, list);
                }
                list.add(message);
            }
        }
    }

    private void doBatchKeyRemove() {
        while(!Thread.interrupted()) {
            try {
                long now = System.currentTimeMillis();
                long elapsed = now - lastKeyRemoveTime;
                if(elapsed >= keyRemoveInterval) {
                    int count = consumeKeyRemoveBuffer();
                    lastKeyRemoveTime = now;
                    long span = System.currentTimeMillis() - now;
                    if(logger.isDebugEnabled()) {
                        logger.debug(String.format("doKeyRemove removed %s keys in %s ms", count, span));
                    }
                    if(count > 100 || span > 100) {
                        logger.warn(String.format("doKeyRemove removed %s keys in %s ms", count, span));
                    }
                } else {
                    Thread.sleep(keyRemoveInterval - elapsed);
                }
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private int consumeKeyRemoveBuffer() {
        Map<String, List<SingleCacheRemoveDTO>> processBuffer = null;
        synchronized(this) {
            if(keyRemoveBuffer != null && !keyRemoveBuffer.isEmpty()) {
                processBuffer = keyRemoveBuffer;
                keyRemoveBuffer = new HashMap<String, List<SingleCacheRemoveDTO>>();
            }
        }
        int count = 0;
        if(processBuffer != null) {
            for(Map.Entry<String, List<SingleCacheRemoveDTO>> entry : processBuffer.entrySet()) {
                List<SingleCacheRemoveDTO> removeKeyList = entry.getValue();
                if(removeKeyList == null || removeKeyList.size() == 0) {
                    logger.error("remove key list for " + entry.getKey() + " is empty");
                    continue;
                }
                count += removeKeyList.size();
                String path = PathUtils.getBatchKeyPath(entry.getKey());
                try {
                    String content = SedesUtils.serialize(removeKeyList);
                    com.dianping.squirrel.client.config.zookeeper.CacheCuratorClientManager.getDefaultClient().update(path, content);
                    if(logger.isDebugEnabled()) {
                        logger.debug("updated key remove path " + path +
                                     ", count " + removeKeyList.size() +
                                     ", size " + content.length());
                    }
                } catch (Exception e) {
                    logger.error("failed to update key remove path " + path, e);
                }
            }
        }
        return count;
    }
}
