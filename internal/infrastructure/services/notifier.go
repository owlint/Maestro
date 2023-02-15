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
	logger   log.Logger
	client   http.Client
	retries  uint
	interval time.Duration
}

func NewHTTPNotifier(
	logger log.Logger,
	timeout time.Duration,
	retries uint,
	interval time.Duration,
) *HTTPNotifier {
	return &HTTPNotifier{
		logger: logger,
		client: http.Client{
			Timeout: timeout,
		},
		retries:  retries,
		interval: interval,
	}
}

func (n *HTTPNotifier) sendNotification(req *http.Request) error {
	resp, err := n.client.Do(req)
	if err != nil {
		return fmt.Errorf("request error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid response status code: %s", resp.Status)
	}

	return nil
}

func (n *HTTPNotifier) Notify(task domain.Task) error {
	taskID := task.TaskID
	callbackURL := task.CallbackURL()

	if callbackURL == "" {
		return nil
	}

	buf := new(bytes.Buffer)
	err := json.NewEncoder(buf).Encode(domain.TaskNotification{
		TaskID: taskID,
	})
	if err != nil {
		return fmt.Errorf("failed to serialize notification body: %w", err)
	}

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

			_ = level.Debug(logger).Log(
				"msg", fmt.Sprintf(
					"Notifying termination of %s to %s",
					taskID, callbackURL,
				),
				"retry", i,
			)

			if err := n.sendNotification(req); err != nil {
				_ = level.Debug(logger).Log(
					"msg", fmt.Sprintf(
						"Error notifying termination of %s to %s: %v",
						taskID, callbackURL, err,
					),
				)
				time.Sleep(n.interval)
				continue
			}

			return
		}

		_ = level.Warn(logger).Log(
			"msg", fmt.Sprintf(
				"Could not notify termination of %s to %s after %d retries",
				taskID, callbackURL, n.retries,
			),
		)
	}()

	return nil
}
