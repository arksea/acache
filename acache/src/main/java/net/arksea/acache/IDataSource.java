package net.arksea.acache;

import scala.concurrent.Future;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public interface IDataSource<TKey, TData> {
    Future<TData> request(TKey key);

    /**
     * 用于判断数据是否已更新
     * @param key
     * @return
     */
    default boolean isUpdated(TKey key,long updateTime) {
        return true;
    }
    default Future<TData> modify(TKey key, IDataModifier<TData> modifier) {
        throw new UnsupportedOperationException();
    }
}
