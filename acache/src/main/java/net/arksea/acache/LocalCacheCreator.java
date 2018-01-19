package net.arksea.acache;

import akka.actor.ActorRef;
import akka.actor.ActorRefFactory;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.dispatch.Mapper;
import akka.pattern.Patterns;
import akka.routing.ConsistentHashingRouter;
import akka.routing.RandomGroup;
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

import static akka.japi.Util.classTag;

/**
 * 从缓存服务读取数据，保存在本地，作为一级缓存
 * Created by xiaohaixing on 2017/4/14.
 */
public class LocalCacheCreator {
    private static final Logger logger = LogManager.getLogger(LocalCacheCreator.class);
    private static final int LOCAL_ASKER_DELAY = 1000; //asker 需要比source多一些的超时时间用于返回本地数据

    public static <TKey,TData> CacheAsker<TKey,TData> createLocalCache(ActorRefFactory actorRefFactory,
                                                                       final ICacheConfig config,
                                                                       final List<String> remoteCacheServerPaths,
                                                                       int timeout, int initTimeout) {
        return createLocalCache(actorRefFactory, config, remoteCacheServerPaths, timeout,initTimeout,
            (source) -> CacheActor.props(source)
        );
    }

    public static <TKey extends ConsistentHashingRouter.ConsistentHashable,TData>
    CacheAsker<TKey,TData> createPooledLocalCache(ActorRefFactory actorRefFactory, int poolSize,
                                                  final ICacheConfig config,
                                                  final List<String> remoteCacheServerPaths,
                                                  int timeout, int initTimeout) {
        return createLocalCache(actorRefFactory, config, remoteCacheServerPaths, timeout,initTimeout,
            (source) -> CacheActor.propsOfCachePool(poolSize, source)
        );
    }

    public static <TKey,TData> CacheAsker<TKey,TData> createLocalCache(ActorRefFactory actorRefFactory,
                                                                       final ICacheConfig config,
                                                                       final List<String> remoteCacheServerPaths,
                                                                       int timeout, int initTimeout,
                                                                       Function<IDataSource,Props> localCacheProps) {
        IDataSource localCacheSource = createLocalCacheSource(actorRefFactory,config,remoteCacheServerPaths,timeout, initTimeout);
        ActorRef localCachePool = actorRefFactory.actorOf(localCacheProps.apply(localCacheSource), config.getCacheName());
        logger.info("Create local cache at：{}",localCachePool.path());
        ActorSelection sel = actorRefFactory.actorSelection(localCachePool.path());
        return new CacheAsker<>(sel, actorRefFactory.dispatcher(), timeout+LOCAL_ASKER_DELAY);
    }

    private static <TKey,TData> IDataSource createLocalCacheSource(ActorRefFactory actorRefFactory,
                                                                   final ICacheConfig config,
                                                                  final List<String> remoteCacheServerPaths,
                                                                  int timeout, int initTimeout) {
        Props serverRouterProps = new RandomGroup(remoteCacheServerPaths).props();
        String routerName = config.getCacheName()+"ServerRouter";
        ActorRef serverRouter = actorRefFactory.actorOf(serverRouterProps, routerName);
        logger.debug("Create cache server router at：{}",serverRouter.path());
        ActorSelection serverRouterSel = actorRefFactory.actorSelection(serverRouter.path());

        final CacheAsker<TKey, TData> asker = new CacheAsker<>(serverRouterSel, actorRefFactory.dispatcher(), timeout);
        //本地缓存向缓存服务请求数据
        IDataSource localCacheSource = new IDataSource<TKey,TData>() {
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
            public Future<TimedData<TData>> request(TKey key) {
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
        return localCacheSource;
    }

    //------------------------------------------------------------------------------------------------------------------

    public static <TKey> CacheAsker<TKey,List> createLocalListCache(ActorRefFactory actorRefFactory,
                                               final String cacheName,
                                               final List<String> remoteCacheServerPaths,
                                               int timeout, int initTimeout) {
        return createLocalListCache(actorRefFactory, cacheName, remoteCacheServerPaths, timeout,initTimeout,
            source -> ListCacheActor.props( source)
        );
    }

    public static <TKey extends ConsistentHashingRouter.ConsistentHashable>
    CacheAsker<TKey,List> createPooledLocalListCache(ActorRefFactory actorRefFactory, int poolSize,
                                                     final String cacheName,
                                                     final List<String> remoteCacheServerPaths,
                                                     int timeout, int initTimeout) {
        return createLocalListCache(actorRefFactory, cacheName, remoteCacheServerPaths, timeout,initTimeout,
            source -> ListCacheActor.propsOfCachePool(poolSize, source)
        );
    }

    public static <TKey> CacheAsker<TKey,List> createLocalListCache(ActorRefFactory actorRefFactory,
                                                                    final String cacheName,
                                                                    final List<String> remoteCacheServerPaths,
                                                                    int timeout, int initTimeout,
                                                                    Function<IDataSource,Props> localCacheProps) {
        IDataSource localCacheSource = createLocalListCacheSource(actorRefFactory,cacheName,remoteCacheServerPaths,timeout, initTimeout);
        ActorRef localCachePool = actorRefFactory.actorOf(localCacheProps.apply(localCacheSource), cacheName);
        logger.info("Create local cache at：{}",localCachePool.path());
        ActorSelection sel = actorRefFactory.actorSelection(localCachePool.path());
        return new CacheAsker<>(sel, actorRefFactory.dispatcher(), timeout+LOCAL_ASKER_DELAY);
    }

    private static <TKey> IDataSource createLocalListCacheSource(ActorRefFactory actorRefFactory,
                                                                 final String cacheName,
                                                                final List<String> remoteCacheServerPaths,
                                                                int timeout, int initTimeout) {
        Props serverRouterProps = new RandomGroup(remoteCacheServerPaths).props();
        String routerName = cacheName+"ServerRouter";
        ActorRef serverRouter = actorRefFactory.actorOf(serverRouterProps, routerName);
        logger.debug("Create cache server router at：{}",serverRouter.path());
        ActorSelection serverRouterSel = actorRefFactory.actorSelection(serverRouter.path());
        final CacheAsker<TKey, List> cacheServerAsker = new CacheAsker<>(serverRouterSel, actorRefFactory.dispatcher(), timeout);
        //本地缓存向缓存服务请求数据
        IDataSource localCacheSource = new IDataSource<TKey,List>() {
            @Override
            public String getCacheName() {
                return cacheName;
            }

            @Override
            public Future<TimedData<List>> request(TKey key) {
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
                GetSize<TKey> getSize = new GetSize(key);
                Future<Integer> futureSize = Patterns.ask(serverRouterSel, getSize, timeout1).mapTo(classTag(Integer.class));
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
        return localCacheSource;
    }

    //------------------------------------------------------------------------------------------------------------------

    /**
     * 为列表类型缓存服务的每个Range创建本地缓存；
     * 对于列表类型的缓存服务，在创建其对应的本地缓存时，如果列表比较长，或者列表各个Range冷热不均，建议用此方法；
     * 创建每个Range的本地缓存，目的是为了防止在缓存过期时每次都更新整个列表
     *
     * @param actorRefFactory
     * @param cacheName
     * @param remoteCacheServerPaths
     * @param timeout
     * @param <TKey>
     * @return
     */
    public static <TKey> CacheAsker<GetRange<TKey>,List> createRangeLocalCache(ActorRefFactory actorRefFactory, final String cacheName, final List<String> remoteCacheServerPaths, int timeout) {
        IDataSource localCacheSource = createRangeServerSource(actorRefFactory,cacheName,remoteCacheServerPaths,timeout);
        ActorRef localCachePool = actorRefFactory.actorOf(CacheActor.props(localCacheSource), cacheName);
        logger.info("Create PooledLocalCache at：{}",localCachePool.path());
        ActorSelection sel = actorRefFactory.actorSelection(localCachePool.path());
        return new CacheAsker<>(sel, actorRefFactory.dispatcher(), timeout+LOCAL_ASKER_DELAY);
    }

    private static <TKey> IDataSource<GetRange<TKey>,List> createRangeServerSource(ActorRefFactory actorRefFactory, final String cacheName, final List<String> remoteCacheServerPaths, int timeout) {
        Props serverRouterProps = new RandomGroup(remoteCacheServerPaths).props();
        String routerName = cacheName+"ServerRouter";
        ActorRef serverRouter = actorRefFactory.actorOf(serverRouterProps, routerName);
        logger.debug("Create cache server router at：{}",serverRouter.path());
        ActorSelection serverRouterSel = actorRefFactory.actorSelection(serverRouter.path());

        final CacheAsker<TKey, List> asker = new CacheAsker<>(serverRouterSel, actorRefFactory.dispatcher(), timeout);
        //本地缓存向缓存服务请求数据
        IDataSource<GetRange<TKey>,List> localCacheSource = new IDataSource<GetRange<TKey>,List>() {
            @Override
            public String getCacheName() {
                return cacheName;
            }

            @Override
            public Future<TimedData<List>> request(GetRange key) {
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
        return localCacheSource;
    }

}
