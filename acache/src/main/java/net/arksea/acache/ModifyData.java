package net.arksea.acache;

/**
 *
 * Created by arksea on 2016/11/21.
 */
public class ModifyData<TKey,TData> implements ICacheRequest<TKey> {
    public final TKey key;
    public final IDataModifier<TData> modifier;
    public ModifyData(TKey key, IDataModifier<TData> modifier) {
        this.key = key;
        this.modifier = modifier;
    }

    @Override
    public Object consistentHashKey() {
        return key;
    }
}
