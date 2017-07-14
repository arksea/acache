package net.arksea.acache;

import scala.concurrent.Future;

import java.util.List;
import java.util.Map;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public interface IDataSource<TKey, TData> {
    Future<TimedData<TData>> request(TKey key);
    default Map<TKey, TimedData<TData>> initCache(List<TKey> keys) {
        return null;
    }
    default Future<TimedData<TData>> modify(TKey key, IDataModifier<TData> modifier) {
        throw new UnsupportedOperationException();
    }
}
