package net.arksea.acache;

import org.apache.commons.lang3.StringUtils;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * Created by xiaohaixing on 2019/2/21.
 */
public abstract class AbstractHitStatService<Key> implements IHitStat<Key> {
    private AtomicLong request = new AtomicLong(0L);
    private AtomicLong hit     = new AtomicLong(0L);
    private AtomicLong expired = new AtomicLong(0L);
    private AtomicLong miss    = new AtomicLong(0L);
    private AtomicLong idleRemoved = new AtomicLong(0L);
    private AtomicLong expiredRemoved = new AtomicLong(0L);
    private AtomicLong size = new AtomicLong(0L);

    protected abstract void doWriteLogs(String body);

    @Override
    public void onRequest(Key key) {
        this.request.incrementAndGet();
    }

    @Override
    public void onHit(Key key) {
        this.hit.incrementAndGet();
    }

    @Override
    public void onExpired(Key key) {
        this.expired.incrementAndGet();
    }

    @Override
    public void onMiss(Key key) {
        this.miss.incrementAndGet();
    }

    @Override
    public void onIdleRemoved(Key key) {
        this.idleRemoved.incrementAndGet();
    }

    @Override
    public void onExpiredRemoved(Key key) {
        this.expiredRemoved.incrementAndGet();
    }

    @Override
    public void setSize(long n) {
        this.size.set(n);
    }

    public void writeLogs() {
        String body = getLogBody();
        if (StringUtils.isNotEmpty(body)) {
            doWriteLogs(body);
        }
    }

    private String getLogBody() {
        StringBuilder sb = new StringBuilder();
        long req = this.request.getAndSet(0L);
        long hit = this.hit.getAndSet(0L);
        long exp = this.expired.getAndSet(0L);
        long miss = this.miss.getAndSet(0L);
        long idleDel = this.idleRemoved.getAndSet(0L);
        long expDel = this.expiredRemoved.getAndSet(0L);
        long size = this.size.get();
        if (req>0 || hit>0 || exp>0 || miss>0) {
            sb.append("hitstat,name=locate")
                    .append(" request=").append(req)
                    .append(",hit=").append(hit)
                    .append(",expired=").append(exp)
                    .append(",miss=").append(miss)
                    .append(",idleDel=").append(idleDel)
                    .append(",expDel=").append(expDel)
                    .append(",size=").append(size)
                    .append("\n");
        }
        return sb.toString();
    }
}
