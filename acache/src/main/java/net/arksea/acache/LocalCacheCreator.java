package net.arksea.acache;

import akka.actor.ActorRef;
import akka.actor.ActorRefFactory;
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

/**
 * 从缓存服务读取数据，保存在本地，作为一级缓存
 * Created by xiaohaixing on 2017/4/14.
 */
public class LocalCacheCreator {
    private static final Logger logger = LogManager.getLogger(LocalCacheCreator.class);
    private static final int LOCAL_ASKER_DELAY = 100; //asker 需要比source多一些的超时时间用于返回本地数据

//    public static <TKey,TData> CacheRefAsker<TKey,TData> createLocalCache(ActorRefFactory actorRefFactory,
//                                                                          final ICacheConfig<TKey> config,
//                                                                          final ActorRef remoteCacheClient,
//                                                                          int timeout, int initTimeout) {
//        final CacheRefAsker<TKey, TData> asker = new CacheRefAsker<>(remoteCacheClient, actorRefFactory.dispatcher(), timeout);
//        IDataSource<TKey,TData> localCacheSource = createLocalCacheSource(actorRefFactory,config,asker,timeout, initTimeout);
//        Props props = CacheActor.props(localCacheSource);
//        ActorRef localCache = actorRefFactory.actorOf(props, config.getCacheName());
//        return new CacheRefAsker<>(localCache, actorRefFactory.dispatcher(), timeout+LOCAL_ASKER_DELAY);
//    }
//
//    public static <TKey extends ConsistentHashingRouter.ConsistentHashable,TData>
//    CacheRefAsker<TKey,TData> createPooledLocalCache(ActorRefFactory actorRefFactory, int poolSize,
//                                                     final ICacheConfig<TKey> config,
//                                                     final ActorRef remoteCacheClient,
//                                                     int timeout, int initTimeout) {
//        final CacheRefAsker<TKey, TData> asker = new CacheRefAsker<>(remoteCacheClient, actorRefFactory.dispatcher(), timeout);
//        IDataSource<TKey,TData> localCacheSource = createLocalCacheSource(actorRefFactory,config,asker,timeout, initTimeout);
//        Props props = CacheActor.propsOfCachePool(poolSize, localCacheSource);
//        ActorRef localCachePool = actorRefFactory.actorOf(props, config.getCacheName());
//        return new CacheRefAsker<>(localCachePool, actorRefFactory.dispatcher(), timeout+LOCAL_ASKER_DELAY);
//    }

    public static <TKey,TData> CacheRefAsker<TKey,TData> createLocalCache(ActorRefFactory actorRefFactory,
                                                                          final ICacheConfig<TKey> config,
                                                                          final ICacheAsker<TKey, TData> remoteCacheAsker,
                                                                          int timeout, int initTimeout) {
        IDataSource<TKey,TData> localCacheSource = createLocalCacheSource(actorRefFactory,config,remoteCacheAsker,timeout, initTimeout);
        Props props = CacheActor.props(localCacheSource);
        ActorRef localCache = actorRefFactory.actorOf(props, config.getCacheName());
        return new CacheRefAsker<>(localCache, actorRefFactory.dispatcher(), timeout+LOCAL_ASKER_DELAY);
    }

    public static <TKey extends ConsistentHashingRouter.ConsistentHashable,TData>
    CacheRefAsker<TKey,TData> createPooledLocalCache(ActorRefFactory actorRefFactory, int poolSize,
                                                     final ICacheConfig<TKey> config,
                                                     final ICacheAsker<TKey, TData> remoteCacheAsker,
                                                     int timeout, int initTimeout) {
        IDataSource<TKey,TData> localCacheSource = createLocalCacheSource(actorRefFactory,config,remoteCacheAsker,timeout, initTimeout);
        Props props = CacheActor.propsOfCachePool(poolSize, localCacheSource);
        ActorRef localCachePool = actorRefFactory.actorOf(props, config.getCacheName());
        return new CacheRefAsker<>(localCachePool, actorRefFactory.dispatcher(), timeout+LOCAL_ASKER_DELAY);
    }

    private static <TKey,TData> IDataSource<TKey,TData> createLocalCacheSource(ActorRefFactory actorRefFactory,
                                                                   final ICacheConfig<TKey> config,
                                                                   final ICacheAsker<TKey,TData> asker,
                                                                  int timeout, int initTimeout) {
        //本地缓存向缓存服务请求数据
        return new IDataSource<TKey,TData>() {
            @Override
            public String getCacheName() {
                return config.getCacheName();
            }
            @Override
            public long getIdleTimeout(TKey key) {
                return config.getIdleTimeout(key);
            }
            @Override
            public long getIdleCleanPeriod() {
                return config.getIdleCleanPeriod();
            }
            @Override
            public long getMaxBackoff() {
                return config.getMaxBackoff();
            }
            @Override
            public boolean waitForRespond() {
                return config.waitForRespond();
            }
            @Override
            public Future<TimedData<TData>> request(ActorRef cacheActor, String cacheName, TKey key) {
                return request(key, timeout);
            }
            @Override
            public Map<TKey, TimedData<TData>> initCache(ActorRef ref) {
                List<TKey> keys = config.getInitKeys();
                if (keys != null && !keys.isEmpty()) {
                    Map<TKey, TimedData<TData>> map = new LinkedHashMap<>(keys.size());
                    for (TKey key : keys) {
                        try {
                            GetData<TKey,TData> get = new GetData<>(key);
                            Future<TimedData<TData>> f = request(key, initTimeout);
                            Duration d = Duration.create(initTimeout,"ms");
                            TimedData<TData> v = Await.result(f, d);
                            map.put(key, v);
                        } catch (Exception ex) {
                            logger.warn("本地缓存({})加载失败:key={}",config.getCacheName(), key.toString(), ex);
                        }
                    }
                    return map;
                } else {
                    return null;
                }
            }
            private Future<TimedData<TData>> request(TKey key, long timeout1) {
                GetData<TKey,TData> get = new GetData<>(key);
                return asker.ask(get,timeout1).map(
                    new Mapper<CacheResponse<TKey, TData>, TimedData<TData>>() {
                        @Override
                        public TimedData<TData> apply(CacheResponse<TKey, TData> it) {
                            if (it.code == ErrorCodes.SUCCEED) {
                                return new TimedData<>(it.expiredTime, it.result);
                            } else if (it.code == ErrorCodes.INVALID_KEY) {
                                return null;
                            } else {
                                throw new CacheAskException("remote cache server error: "+it.code+","+it.msg);
                            }
                        }
                    },
                    actorRefFactory.dispatcher()
                );
            }
        };
    }

    //------------------------------------------------------------------------------------------------------------------

//    public static <TKey> CacheRefAsker<TKey,List> createLocalListCache(ActorRefFactory actorRefFactory,
//                                                                       final String cacheName,
//                                                                       final ActorRef remoteCacheClient,
//                                                                       int timeout, int initTimeout) {
//        final CacheRefAsker<TKey, List> cacheServerAsker = new CacheRefAsker<>(remoteCacheClient, actorRefFactory.dispatcher(), timeout);
//        IDataSource<TKey,List> localCacheSource = createLocalListCacheSource(actorRefFactory,cacheName,cacheServerAsker,timeout, initTimeout);
//        Props props = ListCacheActor.props(localCacheSource);
//        ActorRef localCache = actorRefFactory.actorOf(props, cacheName);
//        return new CacheRefAsker<>(localCache, actorRefFactory.dispatcher(), timeout+LOCAL_ASKER_DELAY);
//
//
//    }
//
//    public static <TKey extends ConsistentHashingRouter.ConsistentHashable>
//    CacheRefAsker<TKey,List> createPooledLocalListCache(ActorRefFactory actorRefFactory, int poolSize,
//                                                        final String cacheName,
//                                                        final ActorRef remoteCacheClient,
//                                                        int timeout, int initTimeout) {
//        final CacheRefAsker<TKey, List> cacheServerAsker = new CacheRefAsker<>(remoteCacheClient, actorRefFactory.dispatcher(), timeout);
//        IDataSource<TKey,List> localCacheSource = createLocalListCacheSource(actorRefFactory,cacheName,cacheServerAsker,timeout, initTimeout);
//        Props props = ListCacheActor.propsOfCachePool(poolSize, localCacheSource);
//        ActorRef localCachePool = actorRefFactory.actorOf(props, cacheName);
//        return new CacheRefAsker<>(localCachePool, actorRefFactory.dispatcher(), timeout+LOCAL_ASKER_DELAY);
//    }

    public static <TKey> CacheRefAsker<TKey,List> createLocalListCache(ActorRefFactory actorRefFactory,
                                                                       final String cacheName,
                                                                       final ICacheAsker<TKey, List> remoteCacheAsker,
                                                                       int timeout, int initTimeout) {
        IDataSource<TKey,List> localCacheSource = createLocalListCacheSource(actorRefFactory,cacheName,remoteCacheAsker,timeout, initTimeout);
        Props props = ListCacheActor.props(localCacheSource);
        ActorRef localCache = actorRefFactory.actorOf(props, cacheName);
        return new CacheRefAsker<>(localCache, actorRefFactory.dispatcher(), timeout+LOCAL_ASKER_DELAY);


    }

    public static <TKey extends ConsistentHashingRouter.ConsistentHashable>
    CacheRefAsker<TKey,List> createPooledLocalListCache(ActorRefFactory actorRefFactory, int poolSize,
                                                        final String cacheName,
                                                        final ICacheAsker<TKey, List> remoteCacheAsker,
                                                        int timeout, int initTimeout) {
        IDataSource<TKey,List> localCacheSource = createLocalListCacheSource(actorRefFactory,cacheName,remoteCacheAsker,timeout, initTimeout);
        Props props = ListCacheActor.propsOfCachePool(poolSize, localCacheSource);
        ActorRef localCachePool = actorRefFactory.actorOf(props, cacheName);
        return new CacheRefAsker<>(localCachePool, actorRefFactory.dispatcher(), timeout+LOCAL_ASKER_DELAY);
    }


    private static <TKey> IDataSource<TKey,List> createLocalListCacheSource(ActorRefFactory actorRefFactory,
                                                                 final String cacheName,
                                                                 final ICacheAsker<TKey, List> cacheServerAsker,
                                                                int timeout, int initTimeout) {
        //本地缓存向缓存服务请求数据
        return new IDataSource<TKey,List>() {
            @Override
            public String getCacheName() {
                return cacheName;
            }

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
                            logger.warn("本地缓存({})加载失败:key={}",cacheName, key.toString(), ex);
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
                Future<Integer> futureSize = cacheServerAsker.getSize(key);
                try {
                    int size = Await.result(futureSize, duration);
                    if (size > 0) {
                        List<Future<TimedData<List>>> futures = new LinkedList<>();
                        for (int index = 0; index < size; index+=COUNT) {
                            GetRange<TKey> getRange = new GetRange<>(key, index, COUNT);
                            Future f = cacheServerAsker.ask(getRange,timeout1).map(
                                new Mapper<CacheResponse<TKey,List>,TimedData<List>>() {
                                    public TimedData<List> apply(CacheResponse<TKey,List> dataResult) {
                                        if (dataResult.code == ErrorCodes.SUCCEED) {
                                            return new TimedData<>(dataResult.expiredTime, dataResult.result);
                                        } else if(dataResult.code == ErrorCodes.INVALID_KEY) {
                                            return null;
                                        } else {
                                            throw new CacheAskException("remote cache server error: "+dataResult.code+","+dataResult.msg);
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
     */
    public static <TKey> CacheRefAsker<GetRange<TKey>,List> createRangeLocalCache(ActorRefFactory actorRefFactory,
                                                                                  final String cacheName,
                                                                                  final ICacheAsker<TKey, List> remoteCacheAsker,
                                                                                  int timeout) {
        IDataSource<GetRange<TKey>,List> localCacheSource = createRangeServerSource(actorRefFactory,cacheName,remoteCacheAsker,timeout);
        ActorRef localCache = actorRefFactory.actorOf(CacheActor.props(localCacheSource), cacheName);
        return new CacheRefAsker<>(localCache, actorRefFactory.dispatcher(), timeout+LOCAL_ASKER_DELAY);
    }

    private static <TKey> IDataSource<GetRange<TKey>,List> createRangeServerSource(ActorRefFactory actorRefFactory,
                                                                                   final String cacheName,
                                                                                   final ICacheAsker<TKey,List> asker,
                                                                                   int timeout) {
        //本地缓存向缓存服务请求数据
        return new IDataSource<GetRange<TKey>,List>() {
            @Override
            public String getCacheName() {
                return cacheName;
            }

            @Override
            public Future<TimedData<List>> request(ActorRef cacheActor, String cacheName, GetRange<TKey> key) {
                return asker.ask(key, timeout).map(
                    new Mapper<CacheResponse<TKey, List>, TimedData<List>>() {
                        @Override
                        public TimedData<List> apply(CacheResponse<TKey, List> it) {
                            if (it.code == ErrorCodes.SUCCEED) {
                                return new TimedData<>(it.expiredTime, it.result);
                            } else if (it.code == ErrorCodes.INVALID_KEY) {
                                return null;
                            } else {
                                throw new CacheAskException("remote cache server error: "+it.code+","+it.msg);
                            }
                        }
                    },
                    actorRefFactory.dispatcher()
                );
            }
        };
    }

}
