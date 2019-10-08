package net.arksea.acache;

/**
 * 接口的实现注意保证线程安全，因为Pool模式时，接口方法会被不同的CacheActor调用
 * Created by xiaohaixing on 2019/2/21.
 */
public interface IHitStat<Key> {
    default void onRequest(Key key){} //请求, 请求数 = Hit + Expired + Miss
    default void onHit(Key key){}     //命中
    default void onExpired(Key key){} //过期
    default void onMiss(Key key){}    //未命中
    default void onIdleRemoved(Key key){}    //闲置移除
    default void onExpiredRemoved(Key key){} //超时移除
    default void setSize(Object tag, long size) { //当Cache为多实例池时，用tag区分来自哪个实例
    }
}
