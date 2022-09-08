package domain

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
)

type TaskState string

func (s TaskState) String() string {
	return string(s)
}

const (
	TaskStatePending   TaskState = "pending"
	TaskStateRunning   TaskState = "running"
	TaskStateCompleted TaskState = "completed"
	TaskStateFailed    TaskState = "failed"
	TaskStateCanceled  TaskState = "canceled"
	TaskStateTimedout  TaskState = "timedout"
)

// Task is a task to be executed
type Task struct {
	TaskID       string
	owner        string
	taskQueue    string
	payload      string
	state        TaskState
	timeout      int32
	retries      int32
	maxRetries   int32
	createdAt    int64
	updatedAt    int64
	notBefore    int64
	startTimeout int32
	result       string
}

// NewTask creates a new task
func NewTask(owner string, taskQueue string, payload string, timeout int32, startTimeout int32, maxRetries int32) *Task {
	now := time.Now().Unix()
	taskID := fmt.Sprintf("Task-%s", uuid.New().String())
	return &Task{
		TaskID:       taskID,
		owner:        owner,
		payload:      payload,
		taskQueue:    taskQueue,
		state:        TaskStatePending,
		timeout:      timeout,
		retries:      0,
		maxRetries:   maxRetries,
		createdAt:    now,
		updatedAt:    now,
		startTimeout: startTimeout,
		notBefore:    now,
	}
}

// NewTask creates a new task
func NewFutureTask(owner string, taskQueue string, payload string, timeout int32, startTimeout int32, maxRetries int32, notBefore int64) (*Task, error) {
	now := time.Now().Unix()
	if notBefore < now {
		return nil, errors.New("notBefore should be in the future")
	}

	taskID := fmt.Sprintf("Task-%s", uuid.New().String())
	return &Task{
		TaskID:       taskID,
		owner:        owner,
		payload:      payload,
		taskQueue:    taskQueue,
		state:        TaskStatePending,
		timeout:      timeout,
		retries:      0,
		maxRetries:   maxRetries,
		createdAt:    now,
		updatedAt:    now,
		startTimeout: startTimeout,
		notBefore:    notBefore,
	}, nil
}

func TaskFromStringMap(data map[string]string) (*Task, error) {
	timeout, err := strconv.ParseInt(data["timeout"], 10, 0)
	if err != nil {
		return nil, err
	}
	startTimeout, err := strconv.ParseInt(data["startTimeout"], 10, 0)
	if err != nil {
		return nil, err
	}
	retries, err := strconv.ParseInt(data["retries"], 10, 0)
	if err != nil {
		return nil, err
	}
	maxRetries, err := strconv.ParseInt(data["maxRetries"], 10, 0)
	if err != nil {
		return nil, err
	}
	createdAt, err := strconv.ParseInt(data["created_at"], 10, 64)
	if err != nil {
		return nil, err
	}
	updatedAt, err := strconv.ParseInt(data["updated_at"], 10, 64)
	if err != nil {
		return nil, err
	}
	notBefore, err := strconv.ParseInt(data["not_before"], 10, 64)
	if err != nil {
		return nil, err
	}

	task := &Task{
		TaskID:       data["task_id"],
		owner:        data["owner"],
		payload:      data["payload"],
		taskQueue:    data["task_queue"],
		state:        TaskState(data["state"]),
		timeout:      int32(timeout),
		startTimeout: int32(startTimeout),
		retries:      int32(retries),
		maxRetries:   int32(maxRetries),
		createdAt:    createdAt,
		updatedAt:    updatedAt,
		notBefore:    notBefore,
	}

	if result, present := data["result"]; present {
		task.result = result
	}

	return task, nil
}

// ObjectID returns the ID of this task
func (t *Task) ObjectID() string {
	return t.TaskID
}

func (t *Task) MaxRetries() int32 {
	return t.maxRetries
}

func (t *Task) CreatedAt() int64 {
	return t.createdAt
}

func (t *Task) UpdatedAt() int64 {
	return t.updatedAt
}

func (t *Task) NotBefore() int64 {
	return t.notBefore
}

func (t *Task) StartTimeout() int32 {
	return t.startTimeout
}

// Owner returns the owner of this task
func (t *Task) Owner() string {
	return t.owner
}

// Queue returns the queue name of this task
func (t *Task) Queue() string {
	return t.taskQueue
}

// Queue returns the queue name of this task
func (t *Task) Payload() string {
	return t.payload
}

// State returns the state of the task
func (t *Task) State() TaskState {
	return TaskState(t.state)
}

func (t *Task) changeState(newState TaskState) {
	if newState == t.state {
		return
	}

	t.state = newState
	t.updated()
}

// Retries returns the number of retries that have been made
func (t *Task) Retries() int32 {
	return t.retries
}

// Select mark a task as selected by a worker
func (t *Task) Select() error {
	if t.state != TaskStatePending {
		return fmt.Errorf("A task can be selected only if it is in pending state : %s", t.state)
	}

	t.changeState(TaskStateRunning)

	return nil
}

// Complete mark a task as completed
func (t *Task) Complete(result string) error {
	if t.state != TaskStateRunning {
		return errors.New("A task can be completed only if it is in running state")
	}

	t.result = result
	t.changeState(TaskStateCompleted)

	return nil
}

// Result returns the result of the task if it is completed and an error otherwise
func (t *Task) Result() (string, error) {
	if t.state != TaskStateCompleted {
		return "", errors.New("You can only have the result of a completed task")
	}
	return t.result, nil
}

// Cancel mark a task as completed
func (t *Task) Cancel() error {
	if t.state != TaskStateRunning && t.state != TaskStatePending {
		return errors.New("A task can be cancelled only if it is in running or pending state")
	}

	t.changeState(TaskStateCanceled)

	return nil
}

// Fail mark a task as failed
func (t *Task) Fail() error {
	if t.state != TaskStateRunning {
		return errors.New("A task can be failed only if it is in running state")
	}

	if t.retries < t.maxRetries {
		t.retry()
	} else {
		t.changeState(TaskStateFailed)
	}

	return nil
}

func (t *Task) GetTimeout() int32 {
	return t.timeout
}

// Timeout mark a task as timedout
func (t *Task) Timeout() error {
	if t.state != TaskStateRunning && t.state != TaskStatePending {
		return fmt.Errorf("Task %s can be timed out only if it is in pending/running state", t.TaskID)
	}

	if t.retries < t.maxRetries && t.state != TaskStatePending {
		t.retry()
	} else {
		t.changeState(TaskStateTimedout)
	}

	return nil
}

func (t *Task) updated() {
	t.updatedAt = time.Now().Unix()
}

func (t *Task) retry() {
	t.retries += 1
	t.changeState(TaskStatePending)
}
