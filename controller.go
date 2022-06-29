package informer_mongo

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	pkg_runtime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

type BusinessAction interface {
	Exec(key string, object interface{}) error
}

// Controller demonstrates how to implement a controller with client-go.
type Controller struct {
	indexer        cache.Indexer
	queue          workqueue.RateLimitingInterface
	informer       cache.Controller
	extraChan      chan watch.Event
	srw            sync.RWMutex
	businessAction BusinessAction
}

// NewController creates a new Controller.
func NewController(collection *mongo.Collection, objType pkg_runtime.Object, newListFunc func(cur *mongo.Cursor) pkg_runtime.Object, action BusinessAction) *Controller {
	// create the pod watcher
	events := make(chan watch.Event)
	listWatchFromMongo := NewListWatchFromMongo(collection, newListFunc, events, fields.Everything())

	// create the workqueue
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	// Bind the workqueue to a cache with the help of an informer. This way we make sure that
	// whenever the cache is updated, the pod key is added to the workqueue.
	// Note that when we finally process the item from the workqueue, we might see a newer version
	// of the Pod than the version which was responsible for triggering the update.
	indexer, informer := cache.NewIndexerInformer(listWatchFromMongo, objType, 0, cache.ResourceEventHandlerFuncs{
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
	}, cache.Indexers{})

	controller := &Controller{}
	controller.indexer = indexer
	controller.extraChan = events
	controller.queue = queue
	controller.informer = informer
	controller.businessAction = action
	return controller
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
		err = c.businessAction.Exec(key.(string), obj)
	}

	// Handle the error if something went wrong during the execution of the business logic
	c.handleErr(err, key)
	return true
}

// handleErr checks if an error happened and makes sure we will retry later.
func (c *Controller) handleErr(err error, key interface{}) {
	if err == nil {
		// Forget about the #AddRateLimited history of the key on every successful synchronization.
		// This ensures that future processing of updates for this key is not delayed because of
		// an outdated error history.
		c.queue.Forget(key)
		return
	}

	// This controller retries 5 times if something goes wrong. After that, it stops trying.
	if c.queue.NumRequeues(key) < 5 {
		klog.Infof("Error syncing pod %v: %v", key, err)

		// Re-enqueue the key rate limited. Based on the rate limiter on the
		// queue and the re-enqueue history, the key will be processed later again.
		c.queue.AddRateLimited(key)
		return
	}

	c.queue.Forget(key)
	// Report to an external entity that, even after several retries, we could not successfully process this key
	runtime.HandleError(err)
	klog.Infof("Dropping pod %q out of the queue: %v", key, err)
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

// NewListWatchFromMongo creates a new ListWatch from the specified mongo client, resource, namespace and field selector.
func NewListWatchFromMongo(c *mongo.Collection, newListFunc func(cur *mongo.Cursor) pkg_runtime.Object, extraChan chan watch.Event, fieldSelector fields.Selector) *cache.ListWatch {
	optionsModifier := func(options *meta_v1.ListOptions) {
		options.FieldSelector = fieldSelector.String()
	}
	listFunc := func(options meta_v1.ListOptions) (pkg_runtime.Object, error) {
		optionsModifier(&options)
		find, err := c.Find(context.Background(), "")
		object := newListFunc(find)
		if err != nil {
			log.Printf("list %s failed: %v", object.GetObjectKind(), err)
		}
		return object, nil
	}
	watchFunc := func(options meta_v1.ListOptions) (watch.Interface, error) {
		options.Watch = true
		optionsModifier(&options)
		watch.NewProxyWatcher(extraChan)
		return watch.NewProxyWatcher(extraChan), nil
	}
	return &cache.ListWatch{ListFunc: listFunc, WatchFunc: watchFunc}
}
