package net.arksea.acache;

/**
 *
 * Created by xiaohaixing on 2018/4/12.
 */
public class HitStat {
    public final long request;
    public final long hit;
    public final long miss;
    public final long expired;

    public HitStat(long request, long hit, long miss, long expired) {
        this.request = request;
        this.hit = hit;
        this.miss = miss;
        this.expired = expired;
    }
}
