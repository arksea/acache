package net.arksea.acache;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public interface ICacheConfig {
    /**
     * 缓存闲置时间，单位秒，超过此时间没有访问将被从内存里清除
     * @return
     */
    long getIdleTimeout();
    /**
     * 缓存激活时间，单位秒，超过此时间CacheActor将调用IDataSource接口取新数据
     * @return
     */
    long getLiveTimeout();


    /**
     * 更新数据的最大退避时间
     * 每次调用IDataSource.request()接口都将递增退避时间，
     * 在退避时间内发起的数据请求将暂时使用过期数据，
     * request调用返回实际的结果后将重置退避时间
     * @return
     */
    long getMaxBackoff();

    /**
     * 集群统一通过Master节点调用IDataSource.request()更新缓存
     * @return
     */
    boolean updateByMaster();

    /**
     * 更新缓存时是否自动同步到集群其他节点
     * @return
     */
    boolean autoSync();
}

