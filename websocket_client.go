package nexus_gorilla

import (
	"encoding/json"
	"sync"

	"github.com/codeus-node/fail"
	"github.com/codeus-node/nexus"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type WsClient struct {
	id           string
	conn         *websocket.Conn
	closedEvents []func() fail.CustomError
	writerMutex  sync.Mutex
}

func NewWsClient(conn *websocket.Conn) nexus.WebsocketClient {
	return &WsClient{
		id:           uuid.NewString(),
		conn:         conn,
		writerMutex:  sync.Mutex{},
		closedEvents: make([]func() fail.CustomError, 0),
	}
}

func (client *WsClient) GetId() string {
	return client.id
}

func (client *WsClient) Send(data []byte) fail.CustomError {
	client.writerMutex.Lock()
	defer client.writerMutex.Unlock()

	err := client.conn.WriteMessage(websocket.TextMessage, data)
	if err != nil {
		return fail.Wrap(err, "Error on sending message to websocket")
	}

	return nil
}

func (client *WsClient) SendJson(data any) fail.CustomError {
	stream, marshalErr := json.Marshal(data)
	if marshalErr != nil {
		return fail.Wrap(marshalErr, "on marshal to json")
	}
	err := client.Send(stream)
	if err != nil {
		return fail.Wrap(err, "on Send bytes to client")
	}
	return nil
}

func (client *WsClient) OnClose(toDo func() fail.CustomError) {
	client.closedEvents = append(client.closedEvents, toDo)
}

func (client *WsClient) Closed() *fail.ErrorList {
	errorList := make([]error, 0)
	for _, toDo := range client.closedEvents {
		err := toDo()
		if err != nil {
			errorList = append(errorList, err)
		}
	}
	return fail.NewErrorList(errorList...)
}

func (client *WsClient) Close() fail.CustomError {
	err := client.conn.Close()
	if err != nil {
		return fail.Wrap(err, "Error on closing connection")
	}

	return nil
}
