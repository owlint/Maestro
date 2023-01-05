package testutils

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"

	"github.com/owlint/maestro/internal/domain"
)

type NotifierSpy struct {
	tasks map[string]domain.Task
}

func NewNotifierSpy() *NotifierSpy {
	return &NotifierSpy{
		tasks: make(map[string]domain.Task),
	}
}

func (n *NotifierSpy) Notify(task domain.Task) error {
	n.tasks[task.TaskID] = task
	return nil
}

func (n *NotifierSpy) Notified(taskID string) bool {
	_, ok := n.tasks[taskID]
	return ok
}

type handlerQueue struct {
	handlers <-chan http.Handler
}

func newHandlerQueue(
	handlers <-chan http.Handler,
) *handlerQueue {
	return &handlerQueue{
		handlers: handlers,
	}
}

func (h *handlerQueue) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h, ok := <-h.handlers; ok {
		h.ServeHTTP(w, r)
	} else {
		http.Error(w, "Handler channel closed", http.StatusInternalServerError)
	}
}

type NotificationTestServer struct {
	server   *httptest.Server
	handlers chan<- http.Handler
}

func NewNotificationTestServer() *NotificationTestServer {
	handlers := make(chan http.Handler, 16)
	return &NotificationTestServer{
		server:   httptest.NewServer(newHandlerQueue(handlers)),
		handlers: handlers,
	}
}

func (s NotificationTestServer) URL() string {
	return s.server.URL
}

func (h *NotificationTestServer) AddHandlerFunc(handler http.HandlerFunc) {
	h.handlers <- http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		handler(w, r)
	})
}

func (h *NotificationTestServer) AddNotificationHandlerFunc(
	handler func(http.ResponseWriter, domain.TaskNotification),
) {
	h.handlers <- http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var n domain.TaskNotification
		err := json.NewDecoder(r.Body).Decode(&n)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		handler(w, n)
	})
}

func (s *NotificationTestServer) Close() {
	s.server.Close()
}
