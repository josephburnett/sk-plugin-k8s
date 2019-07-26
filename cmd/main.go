package main

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/go-plugin"
	k8splugin "github.com/josephburnett/sk-plugin-k8s/pkg/plugin"
	"github.com/josephburnett/sk-plugin/pkg/skplug"
)

type Autoscaler struct {
	hpas    map[string]k8splugin.SkAutoscaler
	hpasMux sync.RWMutex

	i int32
}

func NewAutoscaler() *Autoscaler {
	return &Autoscaler{
		hpas: make(map[string]k8splugin.SkAutoscaler),
		i:    1,
	}
}

func (a *Autoscaler) Create(yaml string, c skplug.Cluster) (key string, err error) {
	key = strconv.Itoa(int(atomic.AddInt32(&a.i, 1)))
	hpa, err := k8splugin.NewSkAutoscaler(yaml)
	if err != nil {
		return "", err
	}
	a.hpasMux.Lock()
	defer a.hpasMux.Unlock()
	a.hpas[key] = hpa
	return key, nil
}

func (a *Autoscaler) Scale(key string) (rec int32, err error) {
	a.hpasMux.RLock()
	hpa, ok := a.hpas[key]
	a.hpasMux.RUnlock()
	if !ok {
		return 0, fmt.Errorf("Autoscaler not found: %v", key)
	}
	// TODO: pass in time
	return hpa.Scale(0)
}

func (a *Autoscaler) Stat(stat []*skplug.Stat) error {
	// TODO: pass through stats
	return nil
}

func (a *Autoscaler) Delete(key string) error {
	a.hpasMux.Lock()
	defer a.hpasMux.Unlock()
	delete(a.hpas, key)
	return nil
}

func main() {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: skplug.Handshake,
		Plugins: map[string]plugin.Plugin{
			"autoscaler": &skplug.AutoscalerPlugin{Impl: NewAutoscaler()},
		},

		// A non-nil value here enables gRPC serving for this plugin...
		GRPCServer: plugin.DefaultGRPCServer,
	})
}
