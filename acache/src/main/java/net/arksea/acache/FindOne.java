package net.arksea.acache;

/**
 *
 * Created by xiaohaixing on 2017/8/11.
 */
public class FindOne<K,V,C> implements ICacheRequest<K,V>  {
    public final K key;
    public final C condition;

    public FindOne(K key, C condition) {
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
