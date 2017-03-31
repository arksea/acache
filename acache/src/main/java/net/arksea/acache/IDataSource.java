package net.arksea.acache;

import scala.concurrent.Future;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public interface IDataSource<TKey, TData> {
    Future<TData> request(TKey key);
    default Future<TData> modify(TKey key, IDataModifier<TData> modifier) {
        throw new UnsupportedOperationException();
    }
}
