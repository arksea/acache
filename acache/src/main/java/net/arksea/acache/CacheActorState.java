package net.arksea.acache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 保存CacheActor的配置、状态与缓存的数据，可以在CacheActor异常重启后继承
 * Created by arksea on 2016/11/17.
 */
public class CacheActorState<TKey, TData> {
    public final Map<TKey, CachedItem<TKey,TData>> cacheMap = new ConcurrentHashMap<>();
    public final IDataSource<TKey,TData> dataSource;
    public CacheActorState(IDataSource<TKey,TData> dataSource) {
        this.dataSource = dataSource;
    }
}
