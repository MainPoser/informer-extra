package informer_mongo

import (
	"errors"
	"fmt"
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
	businessFunc    func(key string, object interface{}) error
	loopDoneErrFunc func(err error, key interface{}, obj interface{})
	requeueTimes    int
}

// NewController creates a new Controller.
func NewController(
	lw cache.ListerWatcher,
	requeueTimes int,
	objType pkg_runtime.Object,
	businessFunc func(key string, object interface{}) error,
	loopDoneErrFunc func(err error, key interface{}, obj interface{}),
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
	return controller
}

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
			// IndexerInformer uses a delta queue, therefore for deletes we have to use this
			// key function.
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key)
			}
		},
	}
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
		fmt.Printf("Pod %s does not exist anymore\n", key)
	} else {
		err = c.businessFunc(key.(string), obj)
	}

	// Handle the error if something went wrong during the execution of the business logic
	c.handleErr(err, key, obj)
	return true
}

// handleErr checks if an error happened and makes sure we will retry later.
func (c *Controller) handleErr(err error, key interface{}, obj interface{}) {
	if err == nil {
		// Forget about the #AddRateLimited history of the key on every successful synchronization.
		// This ensures that future processing of updates for this key is not delayed because of
		// an outdated error history.
		c.queue.Forget(key)
		return
	}

	// This controller retries 5 times if something goes wrong. After that, it stops trying.
	if c.queue.NumRequeues(key) < c.requeueTimes {
		klog.Infof("Error syncing pod %v: %v", key, err)

		// Re-enqueue the key rate limited. Based on the rate limiter on the
		// queue and the re-enqueue history, the key will be processed later again.
		c.queue.AddRateLimited(key)
		return
	}

	c.queue.Forget(key)
	// Report to an external entity that, even after several retries, we could not successfully process this key
	c.loopDoneErrFunc(err, key, obj)
	klog.Infof("Dropping obj %q out of the queue: %v", key, err)
}

// Run begins watching and syncing.
func (c *Controller) Run(workers int, stopCh chan struct{}) {
	defer runtime.HandleCrash()

	// Let the workers stop when we are done
	defer c.queue.ShutDown()
	klog.Info("Starting Pod controller")

	go c.informer.Run(stopCh)

	// Wait for all involved caches to be synced, before processing items from the queue is started
	if !cache.WaitForCacheSync(stopCh, c.informer.HasSynced) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	<-stopCh
	klog.Info("Stopping Pod controller")
}

func (c *Controller) runWorker() {
	for c.processNextItem() {
	}
}

// NewListWatchFromFunc creates a new ListWatch from the specified func
func NewListWatchFromFunc(listFunc cache.ListFunc, watchFunc cache.WatchFunc) *cache.ListWatch {
	return &cache.ListWatch{ListFunc: listFunc, WatchFunc: watchFunc}
}

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
