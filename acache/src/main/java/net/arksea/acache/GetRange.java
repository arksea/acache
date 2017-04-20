package net.arksea.acache;

/**
 * Created by xiaohaixing_dian91 on 2017/3/30.
 */
public class GetRange<TKey,TData> implements ICacheRequest<TKey,TData> {
    public final TKey key;
    public final int start;
    public final int count;

    public GetRange(TKey key,int start,int count) {
        this.key = key;
        this.start = start;
        this.count = count;
    }

    @Override
    public Object consistentHashKey() {
        return key;
    }
}
