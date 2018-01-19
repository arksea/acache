package net.arksea.acache;

import java.io.Serializable;

/**
 *
 * Created by xiaohaixing_dian91 on 2017/5/12.
 */
public class TimedData<TData> implements Serializable {
    final public long time;
    final public TData data;
    final public boolean removeOnExpired; //true时过期将从缓存移除，否则等待idle超时再移除
    public TimedData(final long time, final TData data) {
        this.time = time;
        this.data = data;
        removeOnExpired = false;
    }
    public TimedData(final long time, final TData data, final boolean r) {
        this.time = time;
        this.data = data;
        removeOnExpired = r;
    }
}
