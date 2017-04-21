package net.arksea.acache;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public interface ICacheConfig<TKey> {
    String getCacheName();
    /**
     * 缓存闲置时间，单位秒，超过此时间没有访问将被从内存里清除
     * @return
     */
    default long getIdleTimeout() {
        return 0;
    };
    /**
     * 缓存激活时间，单位秒，超过此时间CacheActor将调用IDataSource接口取新数据
     * @return
     */
    default long getLiveTimeout(TKey key) {
        return 300000; //默认缓存过期时间5分钟
    }

    /**
     * 更新数据的最大退避时间
     * 每次调用IDataSource.request()接口都将递增退避时间，
     * 在退避时间内发起的数据请求将暂时使用过期数据，
     * request调用返回实际的结果后将重置退避时间
     * @return
     */
    default long getMaxBackoff() {
        return 300000; //默认最大退避时间5分钟
    }

    /**
     * 集群统一通过Master节点调用IDataSource.request()更新缓存
     * @return
     */
    default boolean updateByMaster() {
        return false;
    }

    /**
     * 更新缓存时是否自动同步到集群其他节点
     * @return
     */
    default boolean autoSync() {
        return false;
    }

    /**
     * 当数据过期向数据源发起请求时，是否等待数据源返回，如果为false，将先用旧数据返回请求者
     * 当数据源查询返回需要的时间很短，通常都不会超过请求者设置的超时时间的时候，
     * 建议设置为true，这样数据过期时可以立即获得从数据源取到的新数据
     * @return
     */
    default boolean waitForRespond() { return false; }
}

