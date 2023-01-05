package services

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/owlint/maestro/internal/domain"
)

type Notifier interface {
	Notify(task domain.Task) error
}

type HTTPNotifier struct {
	logger  log.Logger
	client  http.Client
	timeout time.Duration
	retries uint
}

func NewHTTPNotifier(
	logger log.Logger,
	timeout time.Duration,
	retries uint,
) *HTTPNotifier {
	return &HTTPNotifier{
		logger: logger,
		client: http.Client{
			Timeout: timeout,
		},
		retries: retries,
	}
}

func (n *HTTPNotifier) Notify(task domain.Task) error {
	taskID := task.TaskID
	callbackURL := task.CallbackURL()

	if callbackURL == "" {
		return nil
	}

	buf := new(bytes.Buffer)
	json.NewEncoder(buf).Encode(domain.TaskNotification{
		TaskID: taskID,
	})

	origReq, err := http.NewRequest(
		http.MethodPost,
		callbackURL,
		http.NoBody,
	)
	if err != nil {
		return fmt.Errorf("failed to build notification request: %w", err)
	}

	go func() {
		logger := log.With(
			n.logger,
			"service", "notifier",
			"taskID", taskID,
			"callbackURL", callbackURL,
		)

		for i := uint(0); i <= n.retries; i++ {
			req := origReq.Clone(origReq.Context())
			req.Body = io.NopCloser(bytes.NewReader(buf.Bytes()))

			level.Debug(logger).Log(
				"msg", fmt.Sprintf(
					"Notifying termination of %s to %s",
					taskID, callbackURL,
				),
				"retry", i,
			)

			resp, err := n.client.Do(req)
			if err != nil {
				level.Debug(logger).Log(
					"msg", fmt.Sprintf(
						"Error in post request to %s: %v",
						callbackURL, err,
					),
				)
				continue
			}
			resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				return
			}

			level.Debug(logger).Log(
				"msg", fmt.Sprintf(
					"Invalid response from %s: status %s",
					callbackURL, resp.Status,
				),
			)
		}

		level.Warn(logger).Log(
			"msg", fmt.Sprintf(
				"Could not notify %s of task %s state change after %d retries",
				callbackURL, taskID, n.retries,
			),
		)
	}()

	return nil
}
