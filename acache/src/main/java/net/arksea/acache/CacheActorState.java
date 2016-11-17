package net.arksea.acache;

import java.util.HashMap;
import java.util.Map;

/**
 * 保存CacheActor的配置、状态与缓存的数据，可以在CacheActor异常重启后继承
 * Created by arksea on 2016/11/17.
 */
public class CacheActorState<TKey, TData> {
    public final Map<TKey, CachedItem<TKey,TData>> cacheMap = new HashMap<>();
    public final ICacheConfig config;
    public final IDataSource<TKey,TData> dataRequestor;
    public CacheActorState(final ICacheConfig config, IDataSource<TKey,TData> dataRequestor) {
        this.config = config;
        this.dataRequestor = dataRequestor;
    }
}
