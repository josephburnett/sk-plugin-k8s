package plugin

import (
	"bytes"
	"fmt"
	"time"

	"github.com/josephburnett/sk-plugin/pkg/skplug/proto"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2beta2"
	"k8s.io/apimachinery/pkg/api/meta/testrestmapper"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	scalefake "k8s.io/client-go/scale/fake"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/podautoscaler/metrics"
	metricsfake "k8s.io/metrics/pkg/client/clientset/versioned/fake"
	cmfake "k8s.io/metrics/pkg/client/custom_metrics/fake"
	emfake "k8s.io/metrics/pkg/client/external_metrics/fake"

	podautoscaler "k8s.io/kubernetes/pkg/controller/podautoscaler"

	_ "k8s.io/kubernetes/pkg/apis/autoscaling/install"
	_ "k8s.io/kubernetes/pkg/apis/extensions/install"
)

type Autoscaler struct {
	controller *podautoscaler.HorizontalController
	hpa        *autoscalingv1.HorizontalPodAutoscaler
	pods       map[string]*proto.Pod
	stats      map[string]*proto.Stat
}

func NewAutoscaler(hpaYaml string) (*Autoscaler, error) {

	client := &fake.Clientset{}
	evtNamespacer := client.CoreV1()
	scaleNamespacer := &scalefake.FakeScaleClient{}
	hpaNamespacer := client.AutoscalingV1()
	mapper := testrestmapper.TestOnlyStaticRESTMapper(legacyscheme.Scheme)
	testMetricsClient := &metricsfake.Clientset{}
	testCMClient := &cmfake.FakeCustomMetricsClient{}
	testEMClient := &emfake.FakeExternalMetricsClient{}
	metricsClient := metrics.NewRESTMetricsClient(
		testMetricsClient.MetricsV1beta1(),
		testCMClient,
		testEMClient,
	)
	informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
	hpaInformer := informerFactory.Autoscaling().V1().HorizontalPodAutoscalers()
	podInformer := informerFactory.Core().V1().Pods()
	resyncPeriod := controller.NoResyncPeriodFunc()
	downscaleStabilizationWindow := 5 * time.Minute
	tolerance := 0.1
	cpuInitializationPeriod := 2 * time.Minute
	delayOfInitialReadinessStatus := 10 * time.Second

	hpa := &autoscalingv2.HorizontalPodAutoscaler{}
	decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader([]byte(hpaYaml)), 1000)
	if err := decoder.Decode(&hpa); err != nil {
		return nil, err
	}
	hpaRaw, err := unsafeConvertToVersionVia(hpa, autoscalingv1.SchemeGroupVersion)
	if err != nil {
		return nil, err
	}
	hpav1 := hpaRaw.(*autoscalingv1.HorizontalPodAutoscaler)

	return &Autoscaler{
		controller: podautoscaler.NewHorizontalController(
			evtNamespacer,
			scaleNamespacer,
			hpaNamespacer,
			mapper,
			metricsClient,
			hpaInformer,
			podInformer,
			resyncPeriod,
			downscaleStabilizationWindow,
			tolerance,
			cpuInitializationPeriod,
			delayOfInitialReadinessStatus,
		),
		hpa:   hpav1,
		pods:  make(map[string]*proto.Pod),
		stats: make(map[string]*proto.Stat),
	}, nil
}

func (a *Autoscaler) Stat(stat []*proto.Stat) error {
	for _, s := range stat {
		a.stats[s.PodName] = s
		// TODO: garbage collect stats after downscale stabilization window.
	}
	return nil
}

func (a *Autoscaler) Scale(now int64) (int32, error) {
	if err := a.controller.ReconcileAutoscaler(a.hpa, "hpa"); err != nil {
		return 0, err
	}
	return a.hpa.Status.DesiredReplicas, nil
}

// Forked from horizontal.go.
func unsafeConvertToVersionVia(obj runtime.Object, externalVersion schema.GroupVersion) (runtime.Object, error) {
	objInt, err := legacyscheme.Scheme.UnsafeConvertToVersion(obj, schema.GroupVersion{Group: externalVersion.Group, Version: runtime.APIVersionInternal})
	if err != nil {
		return nil, fmt.Errorf("failed to convert the given object to the internal version: %v", err)
	}

	objExt, err := legacyscheme.Scheme.UnsafeConvertToVersion(objInt, externalVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to convert the given object back to the external version: %v", err)
	}

	return objExt, err
}
