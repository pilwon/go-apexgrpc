package apexgrpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/apex/go-apex"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type Event struct {
	Package *string          `json:"package"`
	Service *string          `json:"service"`
	Method  *string          `json:"method"`
	Data    *json.RawMessage `json:"data"`
}

type Service struct {
	Desc   *grpc.ServiceDesc
	Server interface{}
}

type MethodID string

func NewMethodID(pkg string, svc string, mtd string) MethodID {
	var id string
	if pkg == "" {
		id = fmt.Sprintf("%s/%s", svc, mtd)
	} else {
		id = fmt.Sprintf("%s.%s/%s", pkg, svc, mtd)
	}
	return MethodID(id)
}

func (id MethodID) String() string {
	return string(id)
}

type handler struct {
	methodDesc *grpc.MethodDesc
	server     interface{}
}

type Server struct {
	handlers map[MethodID]handler
}

func NewServer() *Server {
	return &Server{
		handlers: map[MethodID]handler{},
	}
}

func (s *Server) Register(svcs []Service) {
	for _, svc := range svcs {
		for _, methodDesc := range svc.Desc.Methods {
			uid := NewMethodID("", svc.Desc.ServiceName, methodDesc.MethodName)
			desc := methodDesc
			s.handlers[uid] = handler{
				methodDesc: &desc,
				server:     svc.Server,
			}
		}
	}
}

func (s *Server) Run() {
	s.RunWithContext(context.Background())
}

func (s *Server) RunWithContext(c context.Context) {
	apex.HandleFunc(func(eventMsg json.RawMessage, ctx *apex.Context) (interface{}, error) {
		var event Event
		if err := json.Unmarshal(eventMsg, &event); err != nil {
			return nil, fmt.Errorf("invalid event")
		}
		return s.processEvent(c, &event, ctx)
	})
}

func (s *Server) Invoke(c context.Context, pkg string, svc string, mtd string, data interface{}) (interface{}, error) {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	dataMsg := json.RawMessage(dataBytes)
	event := Event{
		Package: &pkg,
		Service: &svc,
		Method:  &mtd,
		Data:    &dataMsg,
	}
	return s.processEvent(c, &event, nil)
}

func (s *Server) processEvent(c context.Context, event *Event, ctx *apex.Context) (interface{}, error) {
	if event.Package == nil {
		var p string
		event.Package = &p
	}
	if event.Service == nil {
		return nil, fmt.Errorf("event missing service")
	}
	if event.Method == nil {
		return nil, fmt.Errorf("event missing method")
	}
	var data io.Reader
	if event.Data == nil {
		data = strings.NewReader("{}")
	} else {
		data = bytes.NewReader(*event.Data)
	}
	methodID := NewMethodID(*event.Package, *event.Service, *event.Method)
	return s.callGRPCMethod(c, methodID, data)
}

func (s *Server) callGRPCMethod(c context.Context, id MethodID, data io.Reader) (*proto.Message, error) {
	decode := func(v interface{}) error {
		if err := jsonpb.Unmarshal(data, v.(proto.Message)); err != nil {
			return fmt.Errorf("invalid input data for method (%s)", id)
		}
		return nil
	}
	h, ok := s.handlers[id]
	if !ok {
		return nil, fmt.Errorf("method handler not found - %s", id)
	}
	reply, err := h.methodDesc.Handler(h.server, c, decode)
	if err != nil {
		return nil, err
	}
	replyMsg := reply.(proto.Message)
	return &replyMsg, nil
}
