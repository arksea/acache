package net.arksea.acache;

/**
 * Created by xiaohaixing on 2017/8/11.
 */
public class FindAll<K,V,C> implements ICacheRequest<K,V>  {
    public final K key;
    public final C condition;

    public FindAll(K key, C condition) {
        this.key = key;
        this.condition = condition;
    }

    public K getKey() {
        return key;
    }

    @Override
    public Object consistentHashKey() {
        return key;
    }
}
