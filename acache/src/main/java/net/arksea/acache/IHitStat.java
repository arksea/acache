package net.arksea.acache;

/**
 *
 * Created by xiaohaixing on 2019/2/21.
 */
public interface IHitStat<Key> {
    default void onRequest(Key key){}; //请求数 = Hit + Expired + Miss
    default void onHit(Key key){};     //命中次数
    default void onExpired(Key key){}; //过期次数
    default void onMiss(Key key){};    //未命中次数
}
