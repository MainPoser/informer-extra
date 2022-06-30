package informer_mongo

import (
	"fmt"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"strconv"
	"testing"
)

type DemoList struct {
	meta_v1.ObjectMeta
	meta_v1.ListMeta
	Items []Demo
}

func (d *DemoList) GetObjectKind() schema.ObjectKind {
	return d.GetObjectKind()
}

func (d *DemoList) DeepCopyObject() runtime.Object {
	return d
}

type Demo struct {
	meta_v1.ObjectMeta
	Spec string
}

func (d *Demo) GetObjectKind() schema.ObjectKind {
	return d.GetObjectKind()
}

func (d *Demo) DeepCopyObject() runtime.Object {
	return d
}

func TestMongoController(t *testing.T) {
	stopCh := make(chan struct{})
	eventsCh := make(chan watch.Event)
	lf := func(options meta_v1.ListOptions) (runtime.Object, error) {
		// you can list your resource free,just return meta_v1.ListMeta
		ds := make([]Demo, 0)
		for i := 0; i < 100; i++ {
			d := Demo{
				ObjectMeta: meta_v1.ObjectMeta{
					Name: "list" + strconv.Itoa(i),
				},
				Spec: "testing" + strconv.Itoa(i),
			}
			ds = append(ds, d)
		}
		dl := &DemoList{
			ObjectMeta: meta_v1.ObjectMeta{
				Name: "DemoList",
			},
			ListMeta: meta_v1.ListMeta{},
			Items:    ds,
		}
		return dl, nil
	}
	wf := func(options meta_v1.ListOptions) (watch.Interface, error) {
		// custom your watch , watch chan、mongo、mysql、kafka whatever,just return meta_v1.ObjectMeta
		return watch.NewProxyWatcher(eventsCh), nil
	}
	bf := func(key string, object interface{}) error {
		// if return err, will requeue until requeueTimes
		// handle your business logic, just print here
		fmt.Printf("get name %s\n", object.(*Demo).Name)
		return nil
	}
	lef := func(err error, key interface{}, obj interface{}) {
		// when requeueTimes loop done,and err != nil you will handle this
		// just print here
		fmt.Printf("key %s err %v obj %v\n", key, err, obj.(*Demo).Name)
	}
	listWatchFromFunc := NewListWatchFromFunc(lf, wf)
	controller := NewController(listWatchFromFunc, 10, &Demo{},
		bf, lef, nil, nil, nil, true, 0)
	go controller.Run(1, stopCh)

	// put data to eventsCh
	for i := 0; i < 100; i++ {
		e := watch.Event{
			Type: watch.Added,
			Object: &Demo{
				ObjectMeta: meta_v1.ObjectMeta{
					Name: "watch" + strconv.Itoa(i),
				},
				Spec: "",
			},
		}
		eventsCh <- e
	}

	// hold forever
	select {}
}
