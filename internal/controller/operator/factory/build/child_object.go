package build

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	vmv1beta1 "github.com/VictoriaMetrics/operator/api/operator/v1beta1"
	"github.com/VictoriaMetrics/operator/internal/controller/operator/factory/logger"
)

var badObjectsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "operator_bad_objects_count",
		Help: "Number of incorrect objects by controller",
	},
	[]string{"crd", "object_namespace"},
)

func init() {
	metrics.Registry.MustRegister(badObjectsTotal)
}

type scrapeObjectWithStatus interface {
	client.Object
	GetStatusMetadata() *vmv1beta1.StatusMetadata
	AsKey(bool) string
}

type ChildObjects[T scrapeObjectWithStatus] struct {
	built             []T
	broken            []T
	brokenByNamespace map[string]int
	nsn               []string
	zero              T
	name              string
}

func NewChildObjects[T scrapeObjectWithStatus](name string, built []T, nsn []string) *ChildObjects[T] {
	OrderByKeys(built, nsn)
	return &ChildObjects[T]{
		built: built,
		name:  name,
		nsn:   nsn,
	}
}

func (co *ChildObjects[T]) Get(t T) T {
	for _, o := range co.built {
		if o.GetName() == t.GetName() && o.GetNamespace() == t.GetNamespace() {
			return o
		}
	}
	for _, o := range co.broken {
		if o.GetName() == t.GetName() && o.GetNamespace() == t.GetNamespace() {
			return o
		}
	}
	return co.zero
}

func (co *ChildObjects[T]) Broken() []T {
	return co.broken
}

func (co *ChildObjects[T]) All() []T {
	return append(co.built, co.broken...)
}

func (co *ChildObjects[T]) UpdateMetrics(ctx context.Context) {
	logger.SelectedObjects(ctx, co.name, len(co.nsn), len(co.broken), co.nsn)
	for ns, cnt := range co.brokenByNamespace {
		badObjectsTotal.WithLabelValues(co.name, ns).Add(float64(cnt))
	}
}

func (co *ChildObjects[T]) ForEachCollectSkipInvalid(apply func(s T) error) {
	if err := co.forEachCollectSkipOn(apply, func(_ error) bool { return true }); err != nil {
		panic(fmt.Sprintf("BUG: unexpected error: %s", err))
	}
}

func (co *ChildObjects[T]) ForEachCollectSkipNotFound(apply func(s T) error) error {
	return co.forEachCollectSkipOn(apply, IsNotFound)
}

func (co *ChildObjects[T]) setErrorStatus(v T, err error) {
	st := v.GetStatusMetadata()
	st.CurrentSyncError = err.Error()
	co.broken = append(co.broken, v)
	if co.brokenByNamespace == nil {
		co.brokenByNamespace = make(map[string]int)
	}
	co.brokenByNamespace[v.GetNamespace()]++
}

func (co *ChildObjects[T]) forEachCollectSkipOn(apply func(s T) error, shouldIgnoreError func(error) bool) error {
	built := co.built[:0]
	uniqIds := make(map[string]int)
	for _, o := range co.built {
		if err := apply(o); err != nil {
			if !shouldIgnoreError(err) {
				return err
			}
			co.setErrorStatus(o, err)
			continue
		}
		key := o.AsKey(false)
		idx, ok := uniqIds[key]
		if !ok {
			idx = len(built)
			built = append(built, o)
			uniqIds[key] = idx
			continue
		}
		item := built[idx]
		if item.GetCreationTimestamp().After(o.GetCreationTimestamp().Time) {
			item, o = o, item
			built[idx] = o
		}
		err := fmt.Errorf("%s=%s has duplicate id with %s=%s", co.name, o.AsKey(true), co.name, item.AsKey(true))
		co.setErrorStatus(o, err)
	}
	co.built = built
	return nil
}
