package org.example.client.serviceCenter.zkWatcher;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;

import java.util.Arrays;

public class ZkWatcher implements AutoCloseable {
    private static final String RETRY_PATH = "CanRetry";

    private final CuratorFramework client;
    private final ServiceChangeListener listener;
    private CuratorCache curatorCache;

    public ZkWatcher(CuratorFramework client, ServiceChangeListener listener) {
        this.client = client;
        this.listener = listener;
    }

    public void watchToUpdate(String path) {
        curatorCache = CuratorCache.build(client, path);
        curatorCache.listenable().addListener(new CuratorCacheListener() {
            @Override
            public void event(Type type, ChildData oldData, ChildData newData) {
                switch (type) {
                    case NODE_CREATED:
                        handleCreate(newData);
                        break;
                    case NODE_CHANGED:
                        handleReplace(oldData, newData);
                        break;
                    case NODE_DELETED:
                        handleDelete(oldData);
                        break;
                    default:
                        break;
                }
            }
        });
        curatorCache.start();
    }

    private void handleCreate(ChildData data) {
        String[] nodes = parsePath(data);
        if (nodes.length < 2) {
            return;
        }
        if (RETRY_PATH.equals(nodes[0])) {
            if (nodes.length == 3) {
                listener.onRetryAdd(nodes[1], nodes[2]);
            } else if (nodes.length == 2 && isEphemeral(data)) {
                listener.onRetryAdd(nodes[1]);
            }
            return;
        }
        if (nodes.length == 2) {
            listener.onAdd(nodes[0], nodes[1]);
        }
    }

    private void handleReplace(ChildData oldData, ChildData newData) {
        String[] oldNodes = parsePath(oldData);
        String[] newNodes = parsePath(newData);
        if (oldNodes.length != 2 || newNodes.length != 2) {
            return;
        }

        if (RETRY_PATH.equals(oldNodes[0]) && RETRY_PATH.equals(newNodes[0])) {
            if (!oldNodes[1].equals(newNodes[1])) {
                listener.onRetryRemove(oldNodes[1]);
                listener.onRetryAdd(newNodes[1]);
            }
            return;
        }

        if (!RETRY_PATH.equals(oldNodes[0]) && !RETRY_PATH.equals(newNodes[0])) {
            if (oldNodes[0].equals(newNodes[0])) {
                listener.onReplace(oldNodes[0], oldNodes[1], newNodes[1]);
            } else {
                listener.onRemove(oldNodes[0], oldNodes[1]);
                listener.onAdd(newNodes[0], newNodes[1]);
            }
        }
    }

    private void handleDelete(ChildData data) {
        String[] nodes = parsePath(data);
        if (nodes.length < 2) {
            return;
        }
        if (RETRY_PATH.equals(nodes[0])) {
            if (nodes.length == 3) {
                listener.onRetryRemove(nodes[1], nodes[2]);
            } else if (nodes.length == 2 && isEphemeral(data)) {
                listener.onRetryRemove(nodes[1]);
            }
            return;
        }
        if (nodes.length == 2) {
            listener.onRemove(nodes[0], nodes[1]);
        }
    }

    private String[] parsePath(ChildData childData) {
        if (childData == null || childData.getPath() == null || childData.getPath().isEmpty()) {
            return new String[0];
        }
        return Arrays.stream(childData.getPath().split("/"))
                .filter(part -> part != null && !part.isEmpty())
                .toArray(String[]::new);
    }

    private boolean isEphemeral(ChildData childData) {
        return childData != null && childData.getStat() != null
                && childData.getStat().getEphemeralOwner() != 0L;
    }

    @Override
    public void close() {
        if (curatorCache != null) {
            curatorCache.close();
        }
    }
}
