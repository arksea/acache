package net.arksea.acache;

/**
 *
 * Created by xiaohaixing on 2019/2/21.
 */
public interface IHitStat<Key> {
    default void onRequest(Key key){} //请求, 请求数 = Hit + Expired + Miss
    default void onHit(Key key){}     //命中
    default void onExpired(Key key){} //过期
    default void onMiss(Key key){}    //未命中
    default void onIdleRemoved(Key key){}    //闲置移除
    default void onExpiredRemoved(Key key){} //超时移除
    default void setSize(long size) {}       //缓存条数
}
