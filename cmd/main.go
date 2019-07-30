package main

import (
	"fmt"
	"sync"

	"github.com/hashicorp/go-plugin"
	k8splugin "github.com/josephburnett/sk-plugin-k8s/pkg/plugin"
	"github.com/josephburnett/sk-plugin/pkg/skplug"
	"github.com/josephburnett/sk-plugin/pkg/skplug/proto"
)

const (
	pluginType = "hpa.v2beta2.autoscaling.k8s.io"
)

type partition string
type pod_name string

var _ skplug.Plugin = &pluginServer{}

type pluginServer struct {
	mux         sync.RWMutex
	autoscalers map[partition]*k8splugin.Autoscaler
}

func newPluginServer() *pluginServer {
	return &pluginServer{
		autoscalers: make(map[partition]*k8splugin.Autoscaler),
	}
}

func (p *pluginServer) Event(part string, time int64, typ proto.EventType, object skplug.Object) error {
	switch o := object.(type) {
	case *skplug.Autoscaler:
		switch typ {
		case proto.EventType_CREATE:
			return p.createAutoscaler(partition(part), o)
		case proto.EventType_UPDATE:
			return fmt.Errorf("update autoscaler event not supported")
		case proto.EventType_DELETE:
			return p.deleteAutoscaler(partition(part))
		default:
			return fmt.Errorf("unhandled event type: %v", typ)
		}
	case *skplug.Pod:
		// TODO: handle pod CRUD
		return nil
	default:
		return fmt.Errorf("unhandled object type: %T", object)
	}
}

func (p *pluginServer) Stat(part string, stat []*proto.Stat) error {
	p.mux.Lock()
	defer p.mux.Unlock()
	a, ok := p.autoscalers[partition(part)]
	if !ok {
		return fmt.Errorf("stat for non-existant autoscaler partition: %v", part)
	}
	return a.Stat(stat)
}

func (p *pluginServer) Scale(part string, time int64) (rec int32, err error) {
	p.mux.Lock()
	defer p.mux.Unlock()
	a, ok := p.autoscalers[partition(part)]
	if !ok {
		return 0, fmt.Errorf("scale for non-existant autoscaler partition: %v", part)
	}
	return a.Scale(time)
}

func (p *pluginServer) createAutoscaler(part partition, a *skplug.Autoscaler) error {
	if a.Type != pluginType {
		return fmt.Errorf("unsupported autoscaler type %v. this plugin supports %v", a.Type, pluginType)
	}

	p.mux.Lock()
	defer p.mux.Unlock()
	if _, ok := p.autoscalers[part]; ok {
		return fmt.Errorf("duplicate create autoscaler event in partition %v", part)
	}
	autoscaler, err := k8splugin.NewAutoscaler(a.Yaml)
	if err != nil {
		return err
	}
	p.autoscalers[part] = autoscaler
	return nil
}

func (p *pluginServer) deleteAutoscaler(part partition) error {
	p.mux.Lock()
	defer p.mux.Unlock()
	if _, ok := p.autoscalers[part]; !ok {
		return fmt.Errorf("delete autoscaler event for non-existant partition %v", part)
	}
	return nil
}

func main() {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: skplug.Handshake,
		Plugins: map[string]plugin.Plugin{
			"autoscaler": &skplug.AutoscalerPlugin{Impl: newPluginServer()},
		},

		// A non-nil value here enables gRPC serving for this plugin...
		GRPCServer: plugin.DefaultGRPCServer,
	})
}
