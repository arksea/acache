package net.arksea.acache;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public class GetData<TKey,TData> extends CacheRequest<TKey,TData> {
    public GetData(TKey key) {
        super(key);
    }
}
