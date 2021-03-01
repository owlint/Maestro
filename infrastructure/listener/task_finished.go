package listener

import (
	"strings"

	"github.com/owlint/goddd"
	"github.com/owlint/maestro/infrastructure/persistance/repository"
	"github.com/owlint/maestro/pb/taskevents"
	"google.golang.org/protobuf/proto"
)

// TaskFinishedListener is a domain event listener that remove
// task payloads when it finishes
type TaskFinishedListener struct {
	repo repository.PayloadRepository
}

// NewTaskFinishedListener creates a new TaskFinishedListener
func NewTaskFinishedListener(repo repository.PayloadRepository) TaskFinishedListener {
	return TaskFinishedListener{
		repo: repo,
	}
}

// OnEvent removes the existing payload for the task when it finishes
func (l TaskFinishedListener) OnEvent(event goddd.Event) {
	if l.shouldRemove(event) {
		l.repo.Delete(event.ObjectId())
	}
}

func (l TaskFinishedListener) shouldRemove(event goddd.Event) bool {
	appropriateEvent := strings.HasPrefix(event.ObjectId(), "Task-") && event.Name() == "StateChanged"
	isTaskFinished := false

	if appropriateEvent {
		payload := &taskevents.StateChanged{}
		err := proto.Unmarshal(event.Payload(), payload)
		if err == nil {
			state := payload.State
			isTaskFinished = state == "completed" || state == "failed" || state == "timedout"
		}
	}

	return appropriateEvent && isTaskFinished
}
