package nexus_gorilla_test

import (
	"net/http"
	"testing"

	"github.com/codeus-node/fail"
	"github.com/codeus-node/nexus"
	nexus_gorilla "github.com/codeus-node/nexus-gorilla"
)

type HelloEndpoint struct {
	*nexus.BaseEndpoint
}

func (endpoint *HelloEndpoint) Handler(w http.ResponseWriter, _ *http.Request) *nexus.HttpError {
	endpoint.BaseEndpoint.ResponseUtils.WritePlainText(http.StatusOK, "Hello", w)
	return nil
}

func newHelloEndpoint() *HelloEndpoint {
	return &HelloEndpoint{
		BaseEndpoint: nexus.NewBaseEndpoint(),
	}
}

type HelloWebsocket struct {
	*nexus.BaseWebsocket
}

func (socket *HelloWebsocket) Handshake(w http.ResponseWriter, r *http.Request) *nexus.HttpError {
	err := socket.BaseWebsocket.Handshake(w, r)
	if err != nil {
		return err
	}
	return nil
}

func (socket *HelloWebsocket) Handle(data []byte, client nexus.WebsocketClient) fail.CustomError {
	selectedTargets := socket.Pool.Get(func(c nexus.WebsocketClient) bool {
		return c.GetId() == client.GetId()
	})
	for _, target := range selectedTargets {
		err := target.Send([]byte("Hello"))
		if err != nil {
			return err
		}
	}
	return nil
}

func newHelloWebsocket() *HelloWebsocket {
	return &HelloWebsocket{
		BaseWebsocket: nexus.NewBaseWebsocket(),
	}
}

func TestServer(t *testing.T) {
	err := nexus_gorilla.New().Start(nexus.Config{
		ListenAddress: ":8085",
		Middlewares:   nil,
		Endpoints: map[string][]nexus.Endpoint{
			"/api": {
				newHelloEndpoint(),
			},
		},
		StaticFiles: nil,
		CrossOriginRequests: nexus.CorsConfig{
			AllowedOrigins:   []string{"*"},
			AllowedMethods:   []string{"GET", "POST"},
			AllowedHeaders:   []string{"Authorization"},
			AllowCredentials: true,
		},
		Websockets: []nexus.Websocket{
			newHelloWebsocket(),
		},
	})
	if err != nil {
		t.Fatalf("expect no error but was %s", err)
	}
}
