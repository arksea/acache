## ACache - 基于Akka实现的缓存框架

#### 例一、最简单的应用，保存在本地的缓存

##### 1、使用缓存服务
```
@Component
@Transactional
public class PushTargetService {

    @Autowired
    PushTargetDao pushTargetDao;

    @Autowired
    CacheService<String, PushTarget> cacheService;

    private PushTarget save(PushTarget pt) {
        PushTarget n = pushTargetDao.save(pt);
        cacheService.markDirty(pt.getUserId());  //保存数据记录后将其标记为过期
        return n;
    }

    public Future<PushTarget> find(String userId) {
        retrun cacheService.get(userId);
    }
}
```

##### 2、缓存服务初始化

```
@Component
public class PushTargetCacheFactory {
    @Autowired
    ActorSystem system;

    @Autowired
    PushTargetDao pushTargetDao;

    @Bean(name = "pushTargetCache")
    public CacheService<String, PushTarget> createCacheService() {
        PushTargetCacheSource source = new PushTargetCacheSource();
        ActorRef ref = system.actorOf(CacheActor.props(source), "pushTargetCache");
        return new CacheService<PushTargetKey, PushTarget>(ref, system.dispatcher(), 5000);
    }

    class PushTargetCacheSource implements IDataSource<String, PushTarget> {
        @Override
        public Future<TimedData<PushTarget>> request(ActorRef actorRef, String cacheName, String userId) {
            PushTarget t = pushTargetDao.findByProductAndUserId(userId);
            long expiredTime = System.currentTimeMillis() + 3600_000;
            return Futures.successful(new TimedData<>(expiredTime, t));
        }
        @Override
        public String getCacheName() {
            return "pushTargetCache";
        }
        //acache是否等待IDataSource返回，true则等待最新结果返回给调用者，false则在request新数据的同时直接将缓存中已过期的结果返回给调用者
        public boolean waitForRespond() { 
            return true;
        }
        public long getIdleTimeout(PushTargetKey key) { //IDLE清理过期时间
            return 172800_000L; //两天
        }
        public long getIdleCleanPeriod() { //清理过期数据及IDLE数据的周期
            return 3600_000L;//60分钟
        }
    }
}
```
