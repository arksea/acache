package net.arksea.acache;

/**
 * Created by xiaohaixing on 2017/8/11.
 */
public class FindAll<K,V,C> extends CacheRequest<K,V> {
    public final C condition;

    public FindAll(K key, C condition) {
        super(key);
        this.condition = condition;
    }
}
