package nexus_gorilla

import (
	"fmt"
	"log"
	"net/http"
	"slices"
	"strings"

	"github.com/codeus-node/fail"
	di "github.com/codeus-node/generic-di"
	"github.com/codeus-node/icons"
	"github.com/codeus-node/nexus"
	"github.com/codeus-node/printer"
	"github.com/codeus-node/slice_utils"
	"github.com/codeus-node/terminal"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/rs/cors"
)

type Gorilla struct {
	r             *mux.Router
	responseUtils nexus.ResponseUtils
	logger        printer.Logger
}

func (g *Gorilla) Start(config nexus.Config) fail.CustomError {
	for root, endpoints := range config.Endpoints {
		for _, endpoint := range endpoints {
			explicitMiddlewaresToUse := endpoint.ExplicitMiddlewares()
			middlewaresToRegister := []string{}
			if len(explicitMiddlewaresToUse) > 0 {
				middlewaresToRegister = explicitMiddlewaresToUse
			} else {
				middlewaresToRegister = slice_utils.Convert(
					config.Middlewares,
					func(item nexus.Middleware) string { return item.GetKey() })
			}
			g.logger.Log().PrintText(fmt.Sprintf("register Endpoint: [%[3]s %[1]s%[2]s] with Middlewares (%[4]s)",
				root,
				endpoint.GetRoute(),
				strings.Join(endpoint.Methods(), ","),
				strings.Join(middlewaresToRegister, ",")))
			g.r.HandleFunc(fmt.Sprintf("%s%s", root, endpoint.GetRoute()), func(w http.ResponseWriter, r *http.Request) {
				var err *nexus.HttpError

				if !slices.Contains(explicitMiddlewaresToUse, nexus.ExecuteNoMiddleware) {
					if len(explicitMiddlewaresToUse) > 0 && len(config.Middlewares) > 0 {
						for _, middlewareKey := range explicitMiddlewaresToUse {
							for _, middlewareCandidate := range config.Middlewares {
								if middlewareCandidate.GetKey() == middlewareKey {
									if !executedMiddleware(g, middlewareCandidate, w, r) {
										return
									}
									continue
								}
							}
						}
					} else if len(config.Middlewares) > 0 {
						for _, middleware := range config.Middlewares {
							if !executedMiddleware(g, middleware, w, r) {
								return
							}
						}
					}
				}

				err = endpoint.Before(w, r)
				if err != nil {
					endpoint.OnError(err, w, r)
					return
				}

				err = endpoint.Handler(w, r)
				if err != nil {
					endpoint.OnError(err, w, r)
					return
				}

				err = endpoint.After(w, r)
				if err != nil {
					endpoint.OnError(err, w, r)
					return
				}

			}).Methods(endpoint.Methods()...)
		}
	}

	c := cors.New(cors.Options{
		AllowedOrigins:   config.CrossOriginRequests.AllowedOrigins,
		AllowedMethods:   config.CrossOriginRequests.AllowedMethods,
		AllowedHeaders:   config.CrossOriginRequests.AllowedHeaders,
		AllowCredentials: config.CrossOriginRequests.AllowCredentials,
	})

	for _, ws := range config.Websockets {
		g.logger.Log().PrintText(fmt.Sprintf("register Websocket: [%s]", ws.GetRoute()))
		upgrader := websocket.Upgrader{
			ReadBufferSize:    ws.ReadBufferSize(),
			WriteBufferSize:   ws.WriteBufferSize(),
			EnableCompression: ws.EnableCompression(),
			CheckOrigin:       ws.CheckOrigin,
		}

		g.r.HandleFunc(ws.GetRoute(), func(w http.ResponseWriter, r *http.Request) {
			handshakeErr := ws.Handshake(w, r)
			if handshakeErr != nil {
				g.logger.Log().PrintText(fmt.Sprintf("Error on Handshake: %v", handshakeErr))
				return
			}

			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				log.Println("Error on Upgrade:", err)
				return
			}
			defer func() {
				_ = conn.Close()
			}()

			client := NewWsClient(conn)
			connectErr := ws.ClientConnect(client)
			if connectErr != nil {
				client.Close()
				return
			}

			for {
				_, message, readErr := conn.ReadMessage()
				if readErr != nil {
					g.logger.Log().PrintText(fmt.Sprintf("Error on Read: %v", readErr))
					break
				}

				handleMessageErr := ws.Handle(message, client)
				if handleMessageErr != nil {
					g.logger.Log().PrintText(fmt.Sprintf("Error handle message: %v", handleMessageErr))
				}
			}
			client.Closed()
		})
	}

	if config.StaticFiles != nil {
		g.logger.Log().PrintText(fmt.Sprintf("try to link FileSystem %s", config.StaticFiles))
		fileServer := http.FileServer(config.StaticFiles)

		g.r.PathPrefix("/").Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			path := strings.TrimPrefix(r.URL.Path, "/")
			if path == "" {
				path = "index.html"
			}

			f, err := config.StaticFiles.Open(path)
			if err != nil {
				r.URL.Path = "/"
			} else {
				_ = f.Close()
			}

			fileServer.ServeHTTP(w, r)
		}))
	} else {
		g.logger.Log().WithIcon(icons.Warning).WithFormat(terminal.LightYellow).PrintText("no FileSystem linked")
	}

	g.logger.Log().PrintText(fmt.Sprintf("server runs on %s", config.ListenAddress))
	err := http.ListenAndServe(config.ListenAddress, c.Handler(g.r))
	if err != nil {
		return fail.Wrap(err, "on start server")
	}
	return nil
}

func New() nexus.Runner {
	return &Gorilla{
		r:             mux.NewRouter(),
		responseUtils: di.Inject[nexus.ResponseUtils](),
		logger:        di.Inject[printer.Logger](),
	}
}

func executedMiddleware(serverInstance *Gorilla, middleware nexus.Middleware, w http.ResponseWriter, r *http.Request) bool {
	err := middleware.Execute(w, r)
	if err != nil {
		serverInstance.responseUtils.WritePlainText(err.Status, err.Error.Error(), w)
		return false
	}
	return true
}
