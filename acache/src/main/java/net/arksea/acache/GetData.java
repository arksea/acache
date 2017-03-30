package net.arksea.acache;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public class GetData<TKey> implements ICacheRequest<TKey> {
    public final TKey key;

    public GetData(TKey key) {
        this.key = key;
    }

    @Override
    public Object consistentHashKey() {
        return key;
    }
}
