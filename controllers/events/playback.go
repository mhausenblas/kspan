package events

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/weaveworks/libgitops/pkg/serializer"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
)

// Capture details on an object, so we can play back sequences for testing
func (r *EventWatcher) captureObject(obj runtime.Object) {
	if r.Capture == nil {
		return
	}
	fmt.Fprintln(r.Capture, "---")
	fmt.Fprintln(r.Capture, "#", time.Now().Format(time.RFC3339))
	s := serializer.NewSerializer(r.scheme, nil)
	fw := serializer.NewYAMLFrameWriter(r.Capture)
	_ = s.Encoder().Encode(fw, obj)
}

func getInitialObjects(filename string) ([]runtime.Object, error) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	s := serializer.NewSerializer(scheme, nil)
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	fr := serializer.NewYAMLFrameReader(file)
	objects := make(map[types.UID]runtime.Object)
	for {
		obj, err := s.Decoder().Decode(fr)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return nil, err
			}
			break
		}

		ma, _ := meta.Accessor(obj)
		gvk := obj.GetObjectKind().GroupVersionKind()
		if gvk.Kind == "Event" {
			continue
		}
		if _, exists := objects[ma.GetUID()]; exists {
			continue
		}
		fmt.Println(gvk.Kind, ma.GetNamespace(), ma.GetName())
		objects[ma.GetUID()] = obj
	}

	ret := make([]runtime.Object, 0, len(objects))
	for _, v := range objects {
		ret = append(ret, v)
	}
	return ret, nil
}

func playback(ctx context.Context, r *EventWatcher, filename string) {
	s := serializer.NewSerializer(r.scheme, nil)
	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("error opening %q: %v", filename, err)
		return
	}
	defer file.Close()
	fr := serializer.NewYAMLFrameReader(file)
	for {
		obj, err := s.Decoder().Decode(fr)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fmt.Printf("error: %v", err)
			return
		}

		gvk := obj.GetObjectKind().GroupVersionKind()
		if gvk.Kind == "Event" {
			err = r.handleEvent(ctx, obj.(*v1.Event))
		} else {
			err = r.watcher.playback(ctx, obj, r)
		}
		if err != nil {
			fmt.Printf("error: %v", err)
		}
	}
}

func (m *watchManager) playback(ctx context.Context, obj runtime.Object, ew eventNotifier) error {
	ma, _ := meta.Accessor(obj)
	ref := refFromObject(ma)
	m.Lock()
	wi := m.watches[ref]
	m.Unlock()
	if wi == nil {
		return fmt.Errorf("watcher not found for object %q", ref)
	}
	return wi.checkConditionUpdates(obj.(*unstructured.Unstructured), ew)
}
