package net.arksea.acache;

/**
 * Created by xiaohaixing_dian91 on 2016/11/22.
 */
public class DefaultCacheConfig implements ICacheConfig {
    @Override
    public long getIdleTimeout() {
        //默认1小时
        return 3600000;
    }

    @Override
    public long getLiveTimeout() {
        //默认5分钟
        return 300000;
    }

    @Override
    public long getMaxBackoff() {
        //默认3分钟
        return 180000;
    }

    @Override
    public boolean updateByMaster() {
        //默认通过Master更新缓存
        return true;
    }

    @Override
    public boolean autoSync() {
        //默认不自动同步更新到所有节点
        return false;
    }
}
