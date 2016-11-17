package net.arksea.acache;

/**
 *
 * Created by arksea on 2016/11/17.
 */
class CachedItem<TKey, TData> {
    private static final int MIN_RETRY_BACKOFF = 3000; //数据采集失败后的最小退避时间（毫秒）
    public final TKey key;    //缓存的Key
    private TData data;   //缓存的数据
    private long updateTime; //数据更新的时间
    private long requestUpdateTime;  //请求更新的时间
    private long retryBackoff = MIN_RETRY_BACKOFF; //发起更新请求的退避时间

    public CachedItem(final TKey key) {
        this.key = key;
    }

    public TData getData() {
        return data;
    }

    public void setData(TData data) {
        this.updateTime = System.currentTimeMillis();
        this.data = data;
        this.retryBackoff = MIN_RETRY_BACKOFF;
    }

    public void onRequestUpdate(long maxRetryBackoff) {
        this.requestUpdateTime = System.currentTimeMillis();
        this.retryBackoff = Math.min((int) retryBackoff * 2, maxRetryBackoff);
    }

    public boolean isUpdateBackoff() {
        return System.currentTimeMillis() < this.requestUpdateTime + retryBackoff;
    }

    public boolean isTimeout(long timeout) {
        return System.currentTimeMillis() > updateTime + timeout;
    }

}
