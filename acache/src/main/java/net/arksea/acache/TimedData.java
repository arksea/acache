package net.arksea.acache;

import java.io.Serializable;

/**
 *
 * Created by xiaohaixing_dian91 on 2017/5/12.
 */
public class TimedData<TData> implements Serializable {
    final public long time;
    final public TData data;
    public TimedData(final long time, final TData data) {
        this.time = time;
        this.data = data;
    }
}
