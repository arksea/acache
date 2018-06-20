package net.arksea.acache;

import akka.actor.ActorRef;
import akka.actor.ActorRefFactory;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.dispatch.Mapper;
import akka.routing.ConsistentHashingRouter;
import net.arksea.base.FutureUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * 从缓存服务读取数据，保存在本地，作为一级缓存
 * Created by xiaohaixing on 2017/4/14.
 */
public class LocalCacheCreator {
    private static final Logger logger = LogManager.getLogger(LocalCacheCreator.class);
    private static final int LOCAL_ASKER_DELAY = 100; //asker 需要比source多一些的超时时间用于返回本地数据
    public static <TKey,TData> CacheAsker<TKey,TData> createLocalCache(ActorRefFactory actorRefFactory,
                                                                       ICacheConfig<TKey> localCacheConfig,
                                                                       final ICacheAsker<TKey, TData> remoteCacheAsker,
                                                                       int timeout, int initTimeout) {
        return createLocalCache(actorRefFactory, localCacheConfig, remoteCacheAsker, timeout,initTimeout,
            (source) -> CacheActor.props(localCacheConfig, source)
        );
    }
    @Deprecated
    //保持向下兼容性 0.7.3.3
    //use arg 'ICacheAsker<TKey, TData> remoteCacheAsker' instead of 'remoteCacheServerPaths'
    //只会使用remoteCacheServerPaths中的第一个地址
    public static <TKey,TData> CacheAsker<TKey,TData> createLocalCache(ActorRefFactory actorRefFactory,
                                                                       ICacheConfig<TKey> localCacheConfig,
                                                                       final List<String> remoteCacheServerPaths,
                                                                       int timeout, int initTimeout) {
        ActorSelection sel = actorRefFactory.actorSelection(remoteCacheServerPaths.get(0));
        ICacheAsker<TKey, TData> asker = new CacheAsker<>(sel, actorRefFactory.dispatcher(), timeout);
        return createLocalCache(actorRefFactory, localCacheConfig, asker, timeout, initTimeout);
    }

    public static <TKey extends ConsistentHashingRouter.ConsistentHashable,TData>
    CacheAsker<TKey,TData> createPooledLocalCache(ActorRefFactory actorRefFactory, int poolSize,
                                                  ICacheConfig<TKey> localCacheConfig,
                                                  final ICacheAsker<TKey, TData> remoteCacheAsker,
                                                  int timeout, int initTimeout) {
        return createLocalCache(actorRefFactory, localCacheConfig, remoteCacheAsker, timeout,initTimeout,
            (source) -> CacheActor.propsOfCachePool(poolSize, localCacheConfig, source)
        );
    }

    @Deprecated
    //保持向下兼容性 0.7.3.3
    //use arg 'ICacheAsker<TKey, TData> remoteCacheAsker' instead of 'remoteCacheServerPaths'
    //只会使用remoteCacheServerPaths中的第一个地址
    public static <TKey extends ConsistentHashingRouter.ConsistentHashable,TData>
    CacheAsker<TKey,TData> createPooledLocalCache(ActorRefFactory actorRefFactory, int poolSize,
                                                  ICacheConfig<TKey> localCacheConfig,
                                                  final List<String> remoteCacheServerPaths,
                                                  int timeout, int initTimeout) {
        ActorSelection sel = actorRefFactory.actorSelection(remoteCacheServerPaths.get(0));
        ICacheAsker<TKey, TData> asker = new CacheAsker<>(sel, actorRefFactory.dispatcher(), timeout);
        return createPooledLocalCache(actorRefFactory, poolSize, localCacheConfig, asker, timeout, initTimeout);
    }

    private static <TKey,TData> CacheAsker<TKey,TData> createLocalCache(ActorRefFactory actorRefFactory,
                                                                       ICacheConfig<TKey> localCacheConfig,
                                                                       final ICacheAsker<TKey, TData> remoteCacheAsker,
                                                                       int timeout, int initTimeout,
                                                                       Function<IDataSource,Props> localCacheProps) {
        IDataSource localCacheSource = createLocalCacheSource(actorRefFactory,localCacheConfig,remoteCacheAsker,timeout, initTimeout);
        ActorRef localCachePool = actorRefFactory.actorOf(localCacheProps.apply(localCacheSource), localCacheConfig.getCacheName());
        logger.info("Create local cache at：{}",localCachePool.path());
        ActorSelection sel = actorRefFactory.actorSelection(localCachePool.path());
        return new CacheAsker<>(sel, actorRefFactory.dispatcher(), timeout+LOCAL_ASKER_DELAY);
    }

    private static <TKey,TData> IDataSource createLocalCacheSource(ActorRefFactory actorRefFactory,
                                                                  ICacheConfig<TKey> localCacheConfig,
                                                                   final ICacheAsker<TKey, TData> remoteCacheAsker,
                                                                  int timeout, int initTimeout) {
        //本地缓存向缓存服务请求数据
        return new IDataSource<TKey,TData>() {
            @Override
            public Future<TimedData<TData>> request(ActorRef cacheActor, String cacheName, TKey key) {
                return request(key, timeout);
            }
            public Map<TKey, TimedData<TData>> initCache(List<TKey> keys) {
                if (keys != null && !keys.isEmpty()) {
                    Map<TKey, TimedData<TData>> map = new LinkedHashMap<>(keys.size());
                    for (TKey key : keys) {
                        try {
                            Future<TimedData<TData>> f = request(key, initTimeout);
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
            private Future<TimedData<TData>> request(TKey key, long timeout1) {
                GetData<TKey,TData> get = new GetData<>(key);
                return remoteCacheAsker.ask(get,timeout1).map(
                    new Mapper<DataResult<TKey, TData>, TimedData<TData>>() {
                        @Override
                        public TimedData<TData> apply(DataResult<TKey, TData> it) {
                        if (it.failed == null) {
                            return new TimedData<>(it.expiredTime, it.data);
                        } else {
                            throw new CacheSourceException("remote cache server error", it.failed);
                        }
                        }
                    },
                    actorRefFactory.dispatcher()
                );
            }
        };
    }

    //------------------------------------------------------------------------------------------------------------------

    public static <TKey>
    CacheAsker<TKey,List> createLocalListCache(ActorRefFactory actorRefFactory,
                                               ICacheConfig<TKey> localCacheConfig,
                                               final ICacheAsker<TKey, List> remoteCacheAsker,
                                               int timeout, int initTimeout) {
        return createLocalListCache(actorRefFactory, localCacheConfig, remoteCacheAsker, timeout,initTimeout,
            source -> ListCacheActor.props(localCacheConfig, source)
        );
    }

    @Deprecated
    //保持向下兼容性 0.7.3.3
    //use arg 'ICacheAsker<TKey, TData> remoteCacheAsker' instead of 'remoteCacheServerPaths'
    //只会使用remoteCacheServerPaths中的第一个地址
    public static <TKey>
    CacheAsker<TKey,List> createLocalListCache(ActorRefFactory actorRefFactory,
                                               ICacheConfig<TKey> localCacheConfig,
                                               final List<String> remoteCacheServerPaths,
                                               int timeout, int initTimeout) {
        ActorSelection sel = actorRefFactory.actorSelection(remoteCacheServerPaths.get(0));
        ICacheAsker<TKey, List> asker = new CacheAsker<>(sel, actorRefFactory.dispatcher(), timeout);
        return createLocalListCache(actorRefFactory, localCacheConfig, asker, timeout, initTimeout);
    }

    public static <TKey extends ConsistentHashingRouter.ConsistentHashable>
    CacheAsker<TKey,List> createPooledLocalListCache(ActorRefFactory actorRefFactory, int poolSize,
                                                     final ICacheConfig<TKey> localCacheConfig,
                                                     final ICacheAsker<TKey, List> remoteCacheAsker,
                                                     int timeout, int initTimeout) {
        return createLocalListCache(actorRefFactory, localCacheConfig, remoteCacheAsker, timeout,initTimeout,
            source -> ListCacheActor.propsOfCachePool(poolSize, localCacheConfig, source)
        );
    }

    @Deprecated
    //保持向下兼容性 0.7.3.3
    //use arg 'ICacheAsker<TKey, TData> remoteCacheAsker' instead of 'remoteCacheServerPaths'
    //只会使用remoteCacheServerPaths中的第一个地址
    public static <TKey extends ConsistentHashingRouter.ConsistentHashable>
    CacheAsker<TKey,List> createPooledLocalListCache(ActorRefFactory actorRefFactory, int poolSize,
                                                     final ICacheConfig<TKey> localCacheConfig,
                                                     final List<String> remoteCacheServerPaths,
                                                     int timeout, int initTimeout) {
        ActorSelection sel = actorRefFactory.actorSelection(remoteCacheServerPaths.get(0));
        ICacheAsker<TKey, List> asker = new CacheAsker<>(sel, actorRefFactory.dispatcher(), timeout);
        return createPooledLocalListCache(actorRefFactory, poolSize, localCacheConfig, asker, timeout, initTimeout);
    }

    private static <TKey> CacheAsker<TKey,List> createLocalListCache(ActorRefFactory actorRefFactory,
                                                                    ICacheConfig<TKey> localCacheConfig,
                                                                    final ICacheAsker<TKey, List> remoteCacheAsker,
                                                                    int timeout, int initTimeout,
                                                                    Function<IDataSource,Props> localCacheProps) {
        IDataSource localCacheSource = createLocalListCacheSource(actorRefFactory,localCacheConfig,remoteCacheAsker,timeout, initTimeout);
        ActorRef localCachePool = actorRefFactory.actorOf(localCacheProps.apply(localCacheSource), localCacheConfig.getCacheName());
        logger.info("Create local cache at：{}",localCachePool.path());
        ActorSelection sel = actorRefFactory.actorSelection(localCachePool.path());
        return new CacheAsker<>(sel, actorRefFactory.dispatcher(), timeout+LOCAL_ASKER_DELAY);
    }

    private static <TKey> IDataSource createLocalListCacheSource(ActorRefFactory actorRefFactory,
                                                                ICacheConfig<TKey> localCacheConfig,
                                                                 final ICacheAsker<TKey, List> remoteCacheAsker,
                                                                int timeout, int initTimeout) {
        //本地缓存向缓存服务请求数据
        return new IDataSource<TKey,List>() {
            @Override
            public Future<TimedData<List>> request(ActorRef cacheActor, String cacheName, TKey key) {
                return request(key, timeout);
            }
            public Map<TKey, TimedData<List>> initCache(List<TKey> keys) {
                if (keys != null && !keys.isEmpty()) {
                    Map<TKey, TimedData<List>> map = new LinkedHashMap<>(keys.size());
                    for (TKey key : keys) {
                        try {
                            Future<TimedData<List>> f = request(key, initTimeout);
                            Duration d = Duration.create(initTimeout,"ms");
                            TimedData<List> v = Await.result(f, d);
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
            private Future<TimedData<List>> request(TKey key, long timeout1) {
                int COUNT = 20;
                Duration duration = Duration.create(initTimeout,"ms");
                Future<Integer> futureSize = remoteCacheAsker.getSize(key);
                try {
                    int size = Await.result(futureSize, duration);
                    if (size > 0) {
                        List<Future<TimedData<List>>> futures = new LinkedList<>();
                        for (int index = 0; index < size; index+=COUNT) {
                            GetRange<TKey> getRange = new GetRange<>(key, index, COUNT);
                            Future f = remoteCacheAsker.ask(getRange,timeout1).map(
                                new Mapper<DataResult<TKey,List>,TimedData<List>>() {
                                    public TimedData<List> apply(DataResult<TKey,List> dataResult) {
                                    if (dataResult.failed == null) {
                                        return new TimedData<>(dataResult.expiredTime, dataResult.data);
                                    } else {
                                        throw new CacheSourceException("remote cache server error", dataResult.failed);
                                    }
                                    }
                                },
                                actorRefFactory.dispatcher());
                            futures.add(f);
                        }
                        return FutureUtils.mapFutures(futures, actorRefFactory.dispatcher()).map(
                            FutureUtils.mapper(it -> {
                                if (it.size() == 0) {
                                    throw new IllegalStateException("assert failed");
                                }
                                List data = new LinkedList();
                                long time = it.get(0).time;
                                for (TimedData<List> i : it) {
                                    data.addAll(i.data);
                                }
                                logger.debug("更新本地缓存完成：key={}, size={}", key, data.size());
                                return new TimedData<>(time, data);
                            }), actorRefFactory.dispatcher()
                        );
                    } else if (size == 0) {
                        throw new RuntimeException("更新本地缓存失败, 列表长度为0：key="+key);
                    } else {
                        throw new RuntimeException("更新本地缓存失败, 未能获取列表长度：key="+key);
                    }
                } catch(Exception ex) {
                    throw new RuntimeException("更新本地缓存失败, 未能获取列表长度：key="+key, ex);
                }

            }
        };
    }

    //------------------------------------------------------------------------------------------------------------------

    /**
     * 为列表类型缓存服务的每个Range创建本地缓存；
     * 对于列表类型的缓存服务，在创建其对应的本地缓存时，如果列表比较长，或者列表各个Range冷热不均，建议用此方法；
     * 创建每个Range的本地缓存，目的是为了防止在缓存过期时每次都更新整个列表
     *
     * @param actorRefFactory
     * @param localCacheConfig
     * @param remoteCacheAsker
     * @param timeout
     * @param <TKey>
     * @return
     */
    public static <TKey> CacheAsker<GetRange<TKey>,List> createRangeLocalCache(ActorRefFactory actorRefFactory,
                                                                               ICacheConfig<GetRange<TKey>> localCacheConfig,
                                                                               final ICacheAsker<TKey, List> remoteCacheAsker,
                                                                               int timeout) {
        IDataSource localCacheSource = createRangeServerSource(actorRefFactory,remoteCacheAsker,timeout);
        ActorRef localCachePool = actorRefFactory.actorOf(CacheActor.props(localCacheConfig, localCacheSource), localCacheConfig.getCacheName());
        logger.info("Create PooledLocalCache at：{}",localCachePool.path());
        ActorSelection sel = actorRefFactory.actorSelection(localCachePool.path());
        return new CacheAsker<>(sel, actorRefFactory.dispatcher(), timeout+LOCAL_ASKER_DELAY);
    }

    @Deprecated
    //保持向下兼容性 0.7.3.3
    //use arg 'ICacheAsker<TKey, TData> remoteCacheAsker' instead of 'remoteCacheServerPaths'
    //只会使用remoteCacheServerPaths中的第一个地址
    public static <TKey> CacheAsker<GetRange<TKey>,List> createRangeLocalCache(ActorRefFactory actorRefFactory,
                                                                               ICacheConfig<GetRange<TKey>> localCacheConfig,
                                                                               final List<String> remoteCacheServerPaths,
                                                                               int timeout) {
        ActorSelection sel = actorRefFactory.actorSelection(remoteCacheServerPaths.get(0));
        ICacheAsker<TKey, List> asker = new CacheAsker<>(sel, actorRefFactory.dispatcher(), timeout);
        return createRangeLocalCache(actorRefFactory, localCacheConfig, asker, timeout);
    }

    private static <TKey> IDataSource<GetRange<TKey>,List> createRangeServerSource(ActorRefFactory actorRefFactory,
                                                                                   final ICacheAsker<TKey, List> remoteCacheAsker,
                                                                                   int timeout) {
        //本地缓存向缓存服务请求数据
        return new IDataSource<GetRange<TKey>,List>() {
            @Override
            public Future<TimedData<List>> request(ActorRef cacheActor, String cacheName, GetRange<TKey> key) {
                return remoteCacheAsker.ask(key, timeout).map(
                    new Mapper<DataResult<TKey, List>, TimedData<List>>() {
                        @Override
                        public TimedData<List> apply(DataResult<TKey, List> it) {
                            if (it.failed == null) {
                                return new TimedData<>(it.expiredTime, it.data);
                            } else {
                                throw new CacheSourceException("remote cache server error", it.failed);
                            }
                        }
                    },
                    actorRefFactory.dispatcher()
                );
            }
        };
    }

}
