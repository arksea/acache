package net.arksea.acache;

import scala.concurrent.Future;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public interface IDataSource<TKey, TData> {
    Future<TimedData<TData>> request(TKey key);
    default Future<TimedData<TData>> modify(TKey key, IDataModifier<TData> modifier) {
        throw new UnsupportedOperationException();
    }
}
