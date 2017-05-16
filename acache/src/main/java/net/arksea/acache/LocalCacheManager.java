package net.arksea.acache;

import akka.actor.*;
import akka.dispatch.Mapper;
import akka.routing.ConsistentHashingPool;
import akka.routing.ConsistentHashingRouter;
import akka.routing.RandomGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.concurrent.Future;

import java.util.LinkedList;
import java.util.List;

/**
 * 从缓存服务读取数据，保存在本地，作为二级缓存
 * Created by xiaohaixing on 2017/4/14.
 */
public class LocalCacheManager<TKey extends ConsistentHashingRouter.ConsistentHashable,TData> extends UntypedActor {
    private static final Logger logger = LogManager.getLogger(LocalCacheManager.class);
    private CacheAsker<TKey, TData> cacheServerAsker;
    private List<String> remoteCacheServerPaths;
    private final ICacheConfig<TKey> localCacheConfig;
    private final int poolSize;
    private final int timeout;
    public static final String LOCAL_CACHE_POOL_NAME = "localCachePool";

    public LocalCacheManager(int poolSize,ICacheConfig<TKey> localCacheConfig,final List<String> remoteCacheServerPaths,int timeout) {
        this.localCacheConfig = localCacheConfig;
        this.poolSize = poolSize;
        this.remoteCacheServerPaths = remoteCacheServerPaths;
        this.timeout = timeout;
    }

    @Override
    public void preStart() throws Exception {
        logger.info("启动LocalCacheManager:"+ localCacheConfig.getCacheName());
        createLocalCachePool();
    }

    @Override
    public void postStop() throws Exception {
        logger.info("停止LocalCacheManager:"+ localCacheConfig.getCacheName());
    }

    @Override
    public void onReceive(Object o) throws Throwable {
        unhandled(o);
    }

    private CacheAsker<TKey,TData> createRandomAsker(final List<String> remoteCacheServerPaths, int timeout) {
        List<String> remoteCachePoolPaths = new LinkedList<>();
        remoteCacheServerPaths.forEach(it -> {
            remoteCachePoolPaths.add(it+"/pool");
        });
        Props props = new RandomGroup(remoteCachePoolPaths).props();
        String routerName = "cacheServerRouter";
        context().actorOf(props, routerName);
        ActorSelection cacheServerRouter = context().actorSelection(routerName);
        return new CacheAsker<>(cacheServerRouter, context().dispatcher(), timeout);
    }

    private void createLocalCachePool() {
        cacheServerAsker = createRandomAsker(remoteCacheServerPaths, timeout);
        //本地缓存向缓存服务请求数据
        IDataSource localCacheSource = new IDataSource<TKey,TData>() {
            @Override
            public Future<TimedData<TData>> request(TKey key) {
                GetData<TKey,TData> get = new GetData<>(key);
                return cacheServerAsker.ask(get).map(
                    new Mapper<DataResult<TKey,TData>,TimedData<TData>>() {
                        public TimedData<TData> apply(DataResult<TKey,TData> dataResult) {
                            return new TimedData<>(dataResult.expiredTime, dataResult.data);
                        }
                    },context().dispatcher()
                );
            }
        };

        Props props = CacheActor.props(localCacheConfig, localCacheSource);
        ConsistentHashingPool pool = new ConsistentHashingPool(poolSize);
        ActorRef localCachePool = context().actorOf(pool.props(props), LOCAL_CACHE_POOL_NAME);
        logger.info("Create：{}",localCachePool.path());
    }
}
