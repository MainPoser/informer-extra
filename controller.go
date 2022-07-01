package informer_extra

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	pkg_runtime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

// Controller demonstrates how to implement a controller with client-go.
type Controller struct {
	indexer         cache.Indexer
	queue           workqueue.RateLimitingInterface
	informer        cache.Controller
	objType         pkg_runtime.Object
	businessFunc    func(key string, count int, object interface{}) error
	loopDoneErrFunc func(err error, count int, key interface{}, obj interface{})
	requeueTimes    int
}

// NewController creates a new Controller.
func NewController(
	lw cache.ListerWatcher,
	requeueTimes int,
	objType pkg_runtime.Object,
	businessFunc func(key string, count int, object interface{}) error,
	loopDoneErrFunc func(err error, count int, key interface{}, obj interface{}),
	transformer cache.TransformFunc,
	shouldReSync cache.ShouldResyncFunc,
	watchErrorHandler cache.WatchErrorHandler,
	retryOnError bool,
	watchListPageSize int64,
) *Controller {
	// create the workqueue
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	// Bind the workqueue to a cache with the help of an informer. This way we make sure that
	// whenever the cache is updated, the pod key is added to the workqueue.
	// Note that when we finally process the item from the workqueue, we might see a newer version
	// of the Pod than the version which was responsible for triggering the update.
	clientState := cache.NewIndexer(cache.DeletionHandlingMetaNamespaceKeyFunc, cache.Indexers{})
	informer := newInformer(lw, objType, 0, newResourceEventHandlerFunc(queue), clientState, transformer, shouldReSync, watchErrorHandler, retryOnError, watchListPageSize)

	controller := &Controller{}
	controller.indexer = clientState
	controller.queue = queue
	controller.informer = informer
	controller.businessFunc = businessFunc
	controller.requeueTimes = requeueTimes
	controller.loopDoneErrFunc = loopDoneErrFunc
	controller.objType = objType
	return controller
}

// newResourceEventHandlerFunc 创建包内使用的informerHandler
func newResourceEventHandlerFunc(queue workqueue.RateLimitingInterface) cache.ResourceEventHandlerFuncs {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key)
			}
		},
		UpdateFunc: func(old interface{}, new interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(new)
			if err == nil {
				queue.Add(key)
			}
		},
		DeleteFunc: func(obj interface{}) {
			// 直接删除数据，协同逻辑中不处理
		},
	}
}

// newInformer 创建Infromer
func newInformer(
	lw cache.ListerWatcher,
	objType pkg_runtime.Object,
	reSyncPeriod time.Duration,
	h cache.ResourceEventHandler,
	clientState cache.Store,
	transformer cache.TransformFunc,
	shouldReSync cache.ShouldResyncFunc,
	watchErrorHandler cache.WatchErrorHandler,
	retryOnError bool,
	watchListPageSize int64,
) cache.Controller {
	// This will hold incoming changes. Note how we pass clientState in as a
	// KeyLister, that way reSync operations will result in the correct set
	// of update/delete deltas.
	fifo := cache.NewDeltaFIFOWithOptions(cache.DeltaFIFOOptions{
		KnownObjects:          clientState,
		EmitDeltaTypeReplaced: true,
	})

	cfg := &cache.Config{
		Queue:         fifo,
		ListerWatcher: lw,
		Process: func(obj interface{}) error {
			if deltas, ok := obj.(cache.Deltas); ok {
				return processDeltas(h, clientState, transformer, deltas)
			}
			return errors.New("object given as Process argument is not Deltas")
		},
		ObjectType:        objType,
		FullResyncPeriod:  reSyncPeriod,
		ShouldResync:      shouldReSync,
		RetryOnError:      retryOnError,
		WatchErrorHandler: watchErrorHandler,
		WatchListPageSize: watchListPageSize,
	}
	return cache.New(cfg)
}

// Multiplexes updates in the form of a list of Deltas into a Store, and informs
// a given handler of events OnUpdate, OnAdd, OnDelete
func processDeltas(
	// Object which receives event notifications from the given deltas
	handler cache.ResourceEventHandler,
	clientState cache.Store,
	transformer cache.TransformFunc,
	deltas cache.Deltas,
) error {
	// from oldest to newest
	for _, d := range deltas {
		obj := d.Object
		if transformer != nil {
			var err error
			obj, err = transformer(obj)
			if err != nil {
				return err
			}
		}

		switch d.Type {
		case cache.Sync, cache.Replaced, cache.Added, cache.Updated:
			if old, exists, err := clientState.Get(obj); err == nil && exists {
				if err := clientState.Update(obj); err != nil {
					return err
				}
				handler.OnUpdate(old, obj)
			} else {
				if err := clientState.Add(obj); err != nil {
					return err
				}
				handler.OnAdd(obj)
			}
		case cache.Deleted:
			if err := clientState.Delete(obj); err != nil {
				return err
			}
			handler.OnDelete(obj)
		}
	}
	return nil
}

func (c *Controller) processNextItem() bool {
	// Wait until there is a new item in the working queue
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	// Tell the queue that we are done with processing this key. This unblocks the key for other workers
	// This allows safe parallel processing because two pods with the same key are never processed in
	// parallel.
	defer c.queue.Done(key)

	// Invoke the method containing the business logic
	obj, exists, err := c.indexer.GetByKey(key.(string))
	if err != nil {
		klog.Errorf("Fetching object with key %s from store failed with %v", key, err)
		return false
	}
	if !exists {
		// Below we will warm up our cache with a Pod, so that we will see a delete for one pod
		klog.Infof("Pod %s does not exist anymore\n", key)
	} else {
		err = c.businessFunc(key.(string), c.queue.NumRequeues(key), obj)
	}

	// 执行正常，清空key重新入队等信息
	if err == nil {
		// Forget about the #AddRateLimited history of the key on every successful synchronization.
		// This ensures that future processing of updates for this key is not delayed because of
		// an outdated error history.
		c.queue.Forget(key)
		return true
	}

	// 重新入队
	if c.queue.NumRequeues(key) < c.requeueTimes {
		klog.Infof("Error syncing %v %v: %v", c.objType.GetObjectKind(), key, err)

		// Re-enqueue the key rate limited. Based on the rate limiter on the
		// queue and the re-enqueue history, the key will be processed later again.
		c.queue.AddRateLimited(key)
		return true
	}

	// 调用业务逻辑，处理达到最大循环次数仍然无法处理的错误
	c.loopDoneErrFunc(err, c.queue.NumRequeues(key), key, obj)
	// 超过最大重新入对次数，放弃key处理，调用业务循环结束
	c.queue.Forget(key)

	klog.Infof("Dropping obj %q out of the queue: %v", key, err)
	return true
}

// Run begins watching and syncing.
func (c *Controller) Run(workers int, stopCh <-chan struct{}) {
	defer runtime.HandleCrash()

	// Let the workers stop when we are done
	defer c.queue.ShutDown()
	klog.Infof("Starting %s controller", reflect.TypeOf(c.objType).Elem().Name())

	go c.informer.Run(stopCh)

	// Wait for all involved caches to be synced, before processing items from the queue is started
	if !cache.WaitForCacheSync(stopCh, c.informer.HasSynced) {
		runtime.HandleError(errors.New(fmt.Sprintf("Timed out waiting for caches to sync %s", c.objType.GetObjectKind())))
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	<-stopCh
	klog.Infof("Stopping %s controller", reflect.TypeOf(c.objType).Elem().Name())
}

func (c *Controller) runWorker() {
	for c.processNextItem() {
	}
}

// NewListWatchFromFunc creates a new ListWatch from the specified func
func NewListWatchFromFunc(listFunc cache.ListFunc, watchFunc cache.WatchFunc) *cache.ListWatch {
	return &cache.ListWatch{ListFunc: listFunc, WatchFunc: watchFunc}
}
