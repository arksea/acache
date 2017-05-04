package net.arksea.acache;

import akka.actor.*;
import akka.routing.ConsistentHashingPool;
import akka.routing.ConsistentHashingRouter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * Created by xiaohaixing on 2017/3/25.
 */
public class CacheServer<TKey extends ConsistentHashingRouter.ConsistentHashable, TData> extends UntypedActor {
    private static final Logger logger = LogManager.getLogger(CacheServer.class);
    private IDataSource<TKey,TData> cacheSource;
    private ICacheConfig<TKey> cacheConfig;
    private int poolSize;
    public CacheServer(int poolSize, ICacheConfig<TKey> cacheConfig, IDataSource<TKey,TData> cacheSource) {
        this.cacheSource = cacheSource;
        this.cacheConfig = cacheConfig;
        this.poolSize = poolSize;
    }

    @Override
    public void preStart() throws Exception {
        logger.info("启动CacheServer");
        createCachePool();
    }
    private void createCachePool() {
        Props props = CacheActor.props(cacheConfig, cacheSource);
        ConsistentHashingPool pool = new ConsistentHashingPool(poolSize);
        context().actorOf(pool.props(props), "pool");
    }

    @Override
    public void postStop() throws Exception {
        logger.info("停止CacheServer");

    }
    @Override
    public void onReceive(Object o) throws Throwable {
        unhandled(o);
    }
}
