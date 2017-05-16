package net.arksea.acache;

/**
 *
 * Created by arksea on 2016/11/17.
 */
class CachedItem<TKey, TData> {
    private static final int MIN_RETRY_BACKOFF = 3000; //数据采集失败后的最小退避时间（毫秒）
    public final TKey key;    //缓存的Key
    private TimedData<TData> timedData = new TimedData<TData>(0, null);   //缓存的数据
    private long requestUpdateTime;  //请求更新的时间
    private long retryBackoff = MIN_RETRY_BACKOFF; //发起更新请求的退避时间

    public CachedItem(final TKey key) {
        this.key = key;
    }

    public TimedData<TData> getData() {
        return timedData;
    }

    public void setData(TData other, long expiredTime) {
        if (expiredTime > this.timedData.time) {
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

    public boolean isUpdateBackoff() {
        return System.currentTimeMillis() < this.requestUpdateTime + retryBackoff;
    }

    public boolean isExpired() {
        return System.currentTimeMillis() > timedData.time;
    }

}
