package net.arksea.acache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * Created by arksea on 2016/11/17.
 */
class CachedItem<TKey, TData> {
    private static final int MIN_RETRY_BACKOFF = 30000; //数据采集失败后的最小退避时间（毫秒）
    private static final Logger logger = LogManager.getLogger(CachedItem.class);
    public final TKey key;    //缓存的Key
    TimedData<TData> timedData = new TimedData<TData>(0, null);   //缓存的数据
    private long requestUpdateTime;  //请求更新的时间
    private long lastRequestTime;    //最后一次访问时间
    private long retryBackoff = MIN_RETRY_BACKOFF; //发起更新请求的退避时间

    public CachedItem(final TKey key) {
        this.key = key;
        this.lastRequestTime = System.currentTimeMillis();
    }

    //注意，注意，注意： 此处的getData会更新最后缓存访问时间，不适当的访问可能造成idle无法过期
    //命名成这么长也是为了强调这一点 -_-!
    TimedData<TData> getDataAndUpdateLastRequestTime() {
        lastRequestTime = System.currentTimeMillis();
        return timedData;
    }

    public boolean isRemoveOnExpired() {
        return timedData.removeOnExpired;
    }

    public void setData(TData other, long expiredTime) {
        if (expiredTime > this.timedData.time) {
            logger.trace("setData(), expiredTime={}",expiredTime);
            //只有实效性更长的数据才会清除‘数据过期’的状态，并重置退避时间为最小值
            //这样就会有如下效果：
            //   当返回的数据非新数据，cache就会以退避时间周期性的尝试更新数据：3秒、6秒、12秒...
            //   当返回的数据为新数据，cache就会更新数据时间，重置退避时间周期
            this.retryBackoff = MIN_RETRY_BACKOFF;
            this.timedData = new TimedData<>(expiredTime,other);
        }
    }

    public void onRequestUpdate(long maxRetryBackoff) {
        this.requestUpdateTime = System.currentTimeMillis();
        this.retryBackoff = Math.min((int) retryBackoff * 2, maxRetryBackoff);
    }

    /**
     * 标记数据过期时间为0
     */
    public void markDirty() {
        timedData = new TimedData<TData>(0, timedData.data);
    }

    public boolean isUpdateBackoff() {
        return System.currentTimeMillis() < this.requestUpdateTime + retryBackoff;
    }

    public boolean isExpired() {
        return System.currentTimeMillis() > timedData.time;
    }

    public long getLastRequestTime() {
        return lastRequestTime;
    }

}
