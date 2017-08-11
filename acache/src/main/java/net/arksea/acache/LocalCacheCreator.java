package net.arksea.acache;

import akka.actor.ActorRef;
import akka.actor.ActorRefFactory;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.dispatch.Mapper;
import akka.routing.ConsistentHashingPool;
import akka.routing.RandomGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * 从缓存服务读取数据，保存在本地，作为二级缓存
 * Created by xiaohaixing on 2017/4/14.
 */
public class LocalCacheCreator {
    private static final Logger logger = LogManager.getLogger(LocalCacheCreator.class);

    public static <TKey,TData> CacheAsker<TKey,TData> createLocalCache(ActorRefFactory actorRefFactory, ICacheConfig<TKey> localCacheConfig, final List<String> remoteCacheServerPaths, int timeout, int initTimeout) {
        return create(actorRefFactory, localCacheConfig, remoteCacheServerPaths, timeout,initTimeout,
            (IDataSource localCacheSource) -> {
                return CacheActor.props(localCacheConfig, localCacheSource);
            }
        );
    }


    public static <TKey,TData> CacheAsker<TKey,TData> createPooledLocalCache(ActorRefFactory actorRefFactory, int poolSize, ICacheConfig<TKey> localCacheConfig, final List<String> remoteCacheServerPaths, int timeout, int initTimeout) {
        return create(actorRefFactory, localCacheConfig, remoteCacheServerPaths, timeout,initTimeout,
            (IDataSource localCacheSource) -> {
                Props props = CacheActor.props(localCacheConfig, localCacheSource);
                ConsistentHashingPool pool = new ConsistentHashingPool(poolSize);
                return pool.props(props);
            }
        );
    }

    public static <TKey,TData> CacheAsker<TKey,TData> create(ActorRefFactory actorRefFactory, ICacheConfig<TKey> localCacheConfig, final List<String> remoteCacheServerPaths, int timeout, int initTimeout, Function<IDataSource,Props> localCacheProps) {
        IDataSource localCacheSource = createServerSource(actorRefFactory,localCacheConfig,remoteCacheServerPaths,timeout, initTimeout, localCacheProps);
        ActorRef localCachePool = actorRefFactory.actorOf(localCacheProps.apply(localCacheSource), localCacheConfig.getCacheName());
        logger.info("Create PooledLocalCache at：{}",localCachePool.path());
        ActorSelection sel = actorRefFactory.actorSelection(localCachePool.path());
        return new CacheAsker<>(sel, actorRefFactory.dispatcher(), timeout);
    }

    public static <TKey,TData> IDataSource createServerSource(ActorRefFactory actorRefFactory, ICacheConfig<TKey> localCacheConfig, final List<String> remoteCacheServerPaths, int timeout, int initTimeout, Function<IDataSource,Props> localCacheProps) {
        Props serverRouterProps = new RandomGroup(remoteCacheServerPaths).props();
        String routerName = localCacheConfig.getCacheName()+"ServerRouter";
        ActorRef serverRouter = actorRefFactory.actorOf(serverRouterProps, routerName);
        logger.debug("Create cache server router at：{}",serverRouter.path());
        ActorSelection serverRouterSel = actorRefFactory.actorSelection(serverRouter.path());
        final CacheAsker<TKey, TData> cacheServerAsker = new CacheAsker<>(serverRouterSel, actorRefFactory.dispatcher(), timeout);
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
                    },actorRefFactory.dispatcher()
                );
            }

            public Map<TKey, TimedData<TData>> initCache(List<TKey> keys) {
                final CacheAsker<TKey, TData> asker = new CacheAsker<>(serverRouterSel, actorRefFactory.dispatcher(), initTimeout);
                if (keys != null && !keys.isEmpty()) {
                    Map<TKey, TimedData<TData>> map = new LinkedHashMap<>(keys.size());
                    for (TKey key : keys) {
                        try {
                            GetData<TKey,TData> get = new GetData<>(key);
                            Future<TimedData<TData>> f = asker.ask(get).map(
                                new Mapper<DataResult<TKey,TData>,TimedData<TData>>() {
                                    public TimedData<TData> apply(DataResult<TKey,TData> dataResult) {
                                        return new TimedData<>(dataResult.expiredTime, dataResult.data);
                                    }
                                },actorRefFactory.dispatcher()
                            );
                            Duration d = Duration.create(initTimeout,"ms");
                            TimedData<TData> v = Await.result(f, d);
                            map.put(key, v);
                        } catch (Exception ex) {
                            logger.warn("本地缓存({})加载失败:key={}",localCacheConfig.getCacheName(), key.toString(), ex);
                        }
                    }
                    return map;
                } else {
                    return null;
                }
            }
        };
        return localCacheSource;
    }

    public static <TKey,TData> CacheAsker<TKey,TData> createLocalListCache(ActorRefFactory actorRefFactory,
                                                                           ICacheConfig<TKey> localCacheConfig,
                                                                           final List<String> remoteCacheServerPaths,
                                                                           int timeout, int initTimeout) {
        return create(actorRefFactory, localCacheConfig, remoteCacheServerPaths, timeout,initTimeout,
            (IDataSource localCacheSource) -> {
                return ListCacheActor.listCacheProps(localCacheConfig, localCacheSource);
            }
        );
    }

    /**
     * 为列表类型缓存服务的每个Range创建本地缓存
     * 对于列表类型的缓存服务，在创建其对应的本地缓存时，如果列表比较长，建议用此方法，创建每个Range的本地缓存
     * 目的是为了防止在缓存过期时，尝试更新本地缓存的整个列表而造成的数据包过大，也可以避免每次都更新整个列表
     *
     * @param actorRefFactory
     * @param localCacheConfig
     * @param remoteCacheServerPaths
     * @param timeout
     * @param <TKey>
     * @param <TData>
     * @return
     */
    public static <TKey,TData> CacheAsker<GetRange<TKey>,List> createRangeLocalCache(ActorRefFactory actorRefFactory, ICacheConfig<GetRange<TKey>> localCacheConfig, final List<String> remoteCacheServerPaths, int timeout) {
        IDataSource localCacheSource = createRangeServerSource(actorRefFactory,localCacheConfig,remoteCacheServerPaths,timeout);
        ActorRef localCachePool = actorRefFactory.actorOf(CacheActor.props(localCacheConfig, localCacheSource), localCacheConfig.getCacheName());
        logger.info("Create PooledLocalCache at：{}",localCachePool.path());
        ActorSelection sel = actorRefFactory.actorSelection(localCachePool.path());
        return new CacheAsker<>(sel, actorRefFactory.dispatcher(), timeout);
    }

    public static <TKey,TData> IDataSource<GetRange<TKey>,List> createRangeServerSource(ActorRefFactory actorRefFactory, ICacheConfig<GetRange<TKey>> localCacheConfig, final List<String> remoteCacheServerPaths, int timeout) {
        Props serverRouterProps = new RandomGroup(remoteCacheServerPaths).props();
        String routerName = localCacheConfig.getCacheName()+"ServerRouter";
        ActorRef serverRouter = actorRefFactory.actorOf(serverRouterProps, routerName);
        logger.debug("Create cache server router at：{}",serverRouter.path());
        ActorSelection serverRouterSel = actorRefFactory.actorSelection(serverRouter.path());
        //本地缓存向缓存服务请求数据
        IDataSource<GetRange<TKey>,List> localCacheSource = new IDataSource<GetRange<TKey>,List>() {
            @Override
            public Future<TimedData<List>> request(GetRange key) {
                return ask(actorRefFactory, serverRouterSel, key, timeout);
            }
        };
        return localCacheSource;
    }

    public static <K,V> Future<DataResult<K,V>> ask(ActorRefFactory actorRefFactory, ActorSelection cacheActor, ICacheRequest<K,V> req, long timeout) {
        Future<DataResult<K,V>> f = CacheActor.ask(cacheActor, req, timeout);
        return f.map(
            new Mapper<DataResult<K,V>,DataResult<K,V>>() {
                public DataResult<K,V> apply(DataResult<K,V> ret) {
                    if (ret.failed == null) {
                        return ret;
                    } else {
                        throw new RuntimeException(ret.cacheName+"获取数据失败", ret.failed);
                    }
                }
            }, actorRefFactory.dispatcher());
    }
}
