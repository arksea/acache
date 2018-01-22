package net.arksea.acache;

import java.util.List;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public interface ICacheConfig<TKey> {
    String getCacheName();
    /**
     * 缓存闲置时间，单位毫秒，超过此时间没有访问将被从内存里清除
     * 默认为返回0：永不清除
     * @return
     */
    default long getIdleTimeout(TKey key) {
        return 0;
    };
    default long getIdleCleanPeriod() {
        return 0;
    };

    /**
     * 缓存过期是否自动更新
     * @param key
     * @return
     */
    default boolean isAutoUpdateExpiredData(TKey key) {
        return false;
    };
    default long getAutoUpdatePeriod() {
        return 0;
    };
    /**
     * 更新数据的最大退避时间，单位毫秒
     * 每次调用IDataSource.request()接口都将递增退避时间，
     * 在退避时间内发起的数据请求将暂时使用过期数据，
     * request调用返回实际的结果后将重置退避时间
     * @return
     */
    default long getMaxBackoff() {
        return 300000; //默认最大退避时间5分钟
    }

    /**
     * 当数据过期向数据源发起请求时，是否等待数据源返回，如果为false，将先用旧数据返回请求者
     * 当数据源查询返回需要的时间很短，通常都不会超过请求者设置的超时时间的时候，
     * 建议设置为true，这样数据过期时可以立即获得从数据源取到的新数据
     * @return
     */
    default boolean waitForRespond() { return false; }

    /**
     * 定义此接口用于缓存级联，作为上级缓存CacheSource向下级缓存获取初始化数据的依据，
     * 这样上级缓存的实现与封装时无需知道待初始化列表，而推迟到使用此封装时提供
     * 请参考LocalCacheCreator的实现，使用LocalCache方仅提供CacheConfig接口即可使用预先定义好的CacheSorce实现
     * （区别于直接的非级联Cache，使用方是需要提供完整的CacheSource实现的）
     * @return
     */
    default List<TKey> getInitKeys() { return null; }
}
