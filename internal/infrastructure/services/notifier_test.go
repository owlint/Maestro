package services_test

import (
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/owlint/maestro/internal/domain"
	"github.com/owlint/maestro/internal/infrastructure/services"
	"github.com/owlint/maestro/internal/testutils"
	"github.com/stretchr/testify/assert"
)

func TestHTTPNotifier(t *testing.T) {
	server := testutils.NewNotificationTestServer()
	defer server.Close()

	notifs := make(chan domain.TaskNotification)
	defer close(notifs)

	handler := func(w http.ResponseWriter, n domain.TaskNotification) {
		notifs <- n
		w.WriteHeader(http.StatusOK)
	}
	server.AddNotificationHandlerFunc(handler)

	notifier := services.NewHTTPNotifier(log.NewNopLogger(), time.Second, 3)
	task, err := domain.NewTask(
		"laurent",
		"test",
		"payload",
		10, 0, 3,
		server.URL(),
	)
	assert.NoError(t, err)

	err = notifier.Notify(*task)
	assert.NoError(t, err)

	n := <-notifs
	assert.Equal(t, domain.TaskNotification{
		TaskID: task.TaskID,
	}, n)
}

func TestHTTPNotifierRetryOnError(t *testing.T) {
	retries := uint(2)

	server := testutils.NewNotificationTestServer()
	defer server.Close()

	notifs := make(chan domain.TaskNotification)
	defer close(notifs)

	handler := func(w http.ResponseWriter, n domain.TaskNotification) {
		notifs <- n
		w.WriteHeader(http.StatusBadGateway)
	}
	for i := uint(0); i <= retries; i++ {
		server.AddNotificationHandlerFunc(handler)
	}

	notifier := services.NewHTTPNotifier(
		log.NewNopLogger(),
		time.Second,
		retries,
	)
	task, err := domain.NewTask(
		"laurent",
		"test",
		"payload",
		10, 0, 3,
		server.URL(),
	)
	assert.NoError(t, err)

	err = notifier.Notify(*task)
	assert.NoError(t, err)

	for i := uint(0); i <= retries; i++ {
		n := <-notifs
		assert.Equal(t, domain.TaskNotification{
			TaskID: task.TaskID,
		}, n)
	}
}

func TestHTTPNotifierAsync(t *testing.T) {
	server := testutils.NewNotificationTestServer()
	defer server.Close()

	wg := sync.WaitGroup{}
	wg.Add(1)
	defer wg.Done()

	handler := func(w http.ResponseWriter, r *http.Request) {
		// Block request.
		wg.Wait()
		w.WriteHeader(http.StatusOK)
	}
	server.AddHandlerFunc(handler)

	notifier := services.NewHTTPNotifier(
		log.NewNopLogger(),
		10*time.Second,
		0, /* retries */
	)
	task, err := domain.NewTask(
		"laurent",
		"test",
		"payload",
		10, 0, 3,
		server.URL(),
	)
	assert.NoError(t, err)

	// Notify is unaffected by blocking requests, despite the timeout of 10
	// seconds.
	tm := time.Now()
	err = notifier.Notify(*task)
	assert.NoError(t, err)
	assert.Less(t, time.Now().Sub(tm), 1*time.Second)
}

func TestHTTPNotifierRetryAfterTimeout(t *testing.T) {
	retries := uint(2)

	server := testutils.NewNotificationTestServer()
	defer server.Close()

	wg := sync.WaitGroup{}
	wg.Add(1)
	defer wg.Done()

	notifs := make(chan domain.TaskNotification)
	handler := func(w http.ResponseWriter, n domain.TaskNotification) {
		notifs <- n
		// Block request.
		wg.Wait()
		w.WriteHeader(http.StatusOK)
	}
	for i := uint(0); i <= retries; i++ {
		server.AddNotificationHandlerFunc(handler)
	}

	notifier := services.NewHTTPNotifier(
		log.NewNopLogger(),
		100*time.Millisecond,
		retries,
	)
	task, err := domain.NewTask(
		"laurent",
		"test",
		"payload",
		10, 0, 3,
		server.URL(),
	)
	assert.NoError(t, err)

	err = notifier.Notify(*task)
	assert.NoError(t, err)

	// Retries after timeout while requests are blocked by the wait group.
	for i := uint(0); i <= retries; i++ {
		n := <-notifs
		assert.Equal(t, domain.TaskNotification{
			TaskID: task.TaskID,
		}, n)
	}
}
