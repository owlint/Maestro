package domain

import (
	"errors"
	"fmt"
	"time"

	"github.com/owlint/goddd"
	"github.com/owlint/maestro/pb/taskevents"
	"google.golang.org/protobuf/proto"
)

// Task is a task to be executed
type Task struct {
	goddd.EventStream

	TaskID     string
	taskQueue  string
	state      string
	timeout    int32
	retries    int32
	maxRetries int32
	createdAt  int64
	updatedAt  int64
}

// NewTask creates a new task
func NewTask(taskQueue string, timeout int32, maxRetries int32) *Task {
	stream := goddd.NewEventStream()
	t := Task{
		EventStream: &stream,
	}

	identity := goddd.NewIdentity("Task")
	t.TaskID = identity
	t.AddEvent(&t, "TaskIDSet", &taskevents.TaskIDSet{TaskID: identity})
	t.AddEvent(&t, "TaskQueueChanged", &taskevents.TaskQueueChanged{TaskQueue: taskQueue})
	t.AddEvent(&t, "StateChanged", &taskevents.StateChanged{State: "pending"})
	t.AddEvent(&t, "TimeoutChanged", &taskevents.TimeoutChanged{Timeout: timeout})
	t.AddEvent(&t, "MaxRetriesSet", &taskevents.MaxRetriesSet{MaxRetries: maxRetries})
	t.AddEvent(&t, "Created", &taskevents.Created{Timestamp: time.Now().Unix()})
	t.AddEvent(&t, "Updated", &taskevents.Updated{Timestamp: time.Now().Unix()})

	return &t
}

// ObjectID returns the ID of this task
func (t *Task) ObjectID() string {
	return t.TaskID
}

// State returns the state of the task
func (t *Task) State() string {
	return t.state
}

// Retries returns the number of retries that have been made
func (t *Task) Retries() int32 {
	return t.retries
}

// Select mark a task as selected by a worker
func (t *Task) Select() error {
	if t.state != "pending" {
		return errors.New("A task can be selected only if it is in pending state")
	}

	t.AddEvent(t, "StateChanged", &taskevents.StateChanged{State: "running"})
	t.updated()

	return nil
}

// Complete mark a task as completed
func (t *Task) Complete() error {
	if t.state != "running" {
		return errors.New("A task can be completed only if it is in running state")
	}

	t.AddEvent(t, "StateChanged", &taskevents.StateChanged{State: "completed"})
	t.updated()

	return nil
}

// Fail mark a task as failed
func (t *Task) Fail() error {
	if t.state != "running" {
		return errors.New("A task can be failed only if it is in running state")
	}

	if t.retries < t.maxRetries {
		t.retry()
	} else {
		t.AddEvent(t, "StateChanged", &taskevents.StateChanged{State: "failed"})
		t.updated()
	}

	return nil
}

// Timeout mark a task as timedout
func (t *Task) Timeout() error {
	if t.state != "running" {
		return errors.New("A task can be timed out only if it is in running state")
	}

	if t.retries < t.maxRetries {
		t.retry()
	} else {
		t.AddEvent(t, "StateChanged", &taskevents.StateChanged{State: "timedout"})
		t.updated()
	}

	return nil
}

func (t *Task) updated() {
	t.AddEvent(t, "Updated", &taskevents.Updated{Timestamp: time.Now().Unix()})
}

func (t *Task) retry() {
	t.AddEvent(t, "StateChanged", &taskevents.StateChanged{State: "pending"})
	t.AddEvent(t, "Retried", &taskevents.Retried{Retries: t.retries + 1})
	t.updated()
}

// Apply applies to given event to the object
func (t *Task) Apply(eventName string, event []byte) error {
	switch eventName {
	case "TaskIDSet":
		payload := &taskevents.TaskIDSet{}
		err := proto.Unmarshal(event, payload)
		if err != nil {
			return err
		}
		t.TaskID = payload.TaskID
	case "StateChanged":
		payload := &taskevents.StateChanged{}
		err := proto.Unmarshal(event, payload)
		if err != nil {
			return err
		}
		t.state = payload.State
	case "TimeoutChanged":
		payload := &taskevents.TimeoutChanged{}
		err := proto.Unmarshal(event, payload)
		if err != nil {
			return err
		}
		t.timeout = payload.Timeout
	case "MaxRetriesSet":
		payload := &taskevents.MaxRetriesSet{}
		err := proto.Unmarshal(event, payload)
		if err != nil {
			return err
		}
		t.maxRetries = payload.MaxRetries
	case "Created":
		payload := &taskevents.Created{}
		err := proto.Unmarshal(event, payload)
		if err != nil {
			return err
		}
		t.createdAt = payload.Timestamp
	case "Updated":
		payload := &taskevents.Updated{}
		err := proto.Unmarshal(event, payload)
		if err != nil {
			return err
		}
		t.updatedAt = payload.Timestamp
	case "Retried":
		payload := &taskevents.Retried{}
		err := proto.Unmarshal(event, payload)
		if err != nil {
			return err
		}
		t.retries = payload.Retries
	case "TaskQueueChanged":
		payload := &taskevents.TaskQueueChanged{}
		err := proto.Unmarshal(event, payload)
		if err != nil {
			return err
		}
		t.taskQueue = payload.TaskQueue
	default:
		return fmt.Errorf("Unknown event %s", eventName)
	}

	return nil
}
