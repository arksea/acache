package net.arksea.acache;

import net.arksea.base.ServiceResponse;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public class CacheResponse<TKey,TData> extends ServiceResponse<TData> {
    public final String cacheName;
    public final TKey key;
    public final long expiredTime;
    public CacheResponse(int code, String msg, String reqid, TKey key, TData result, String cacheName, final long time) {
        super(code,msg,reqid,result);
        this.expiredTime = time;
        this.cacheName = cacheName;
        this.key = key;
    }
    public CacheResponse(int code, String msg, String reqid, TKey key, String cacheName) {
        super(code,msg,reqid);
        this.expiredTime = 0;
        this.cacheName = cacheName;
        this.key = key;
    }
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("cacheName=").append(cacheName).append(",code=").append(code).append(",msg=").append(msg)
          .append(",reqid=").append(reqid).append(",result=").append(result);
        return sb.toString();
    }
}
