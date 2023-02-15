package domain

import (
	"errors"
	"fmt"
	"net/url"
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

type TaskEvent struct {
	TaskID       string    `json:"task_id"`
	OwnerID      string    `json:"owner_id"`
	State        TaskState `json:"state"`
	Version      int       `json:"version"`
	TaskQueue    string    `json:"task_queue"`
	Timeout      int32     `json:"timeout"`
	Retries      int32     `json:"retries"`
	MaxRetries   int32     `json:"max_retries"`
	NotBefore    int64     `json:"not_before"`
	StartTimeout int32     `json:"start_timeout"`
}

type TaskNotification struct {
	TaskID string `json:"task_id"`
}

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
	version      int
	callbackURL  string

	events []TaskEvent
}

// NewTask creates a new task
func NewTask(
	owner string,
	taskQueue string,
	payload string,
	timeout int32,
	startTimeout int32,
	maxRetries int32,
	callbackURL string,
) (*Task, error) {
	if _, err := url.Parse(callbackURL); err != nil {
		return nil, fmt.Errorf("invalid callback URL: %w", err)
	}

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
		callbackURL:  callbackURL,
		events: []TaskEvent{{
			TaskID:       taskID,
			OwnerID:      owner,
			State:        TaskStatePending,
			TaskQueue:    taskQueue,
			Timeout:      timeout,
			Retries:      0,
			MaxRetries:   maxRetries,
			StartTimeout: startTimeout,
			NotBefore:    now,
		}},
	}, nil
}

// NewTask creates a new task
func NewFutureTask(
	owner string,
	taskQueue string,
	payload string,
	timeout int32,
	startTimeout int32,
	maxRetries int32,
	notBefore int64,
	callbackURL string,
) (*Task, error) {
	now := time.Now().Unix()
	if notBefore < now {
		return nil, errors.New("notBefore should be in the future")
	}

	if _, err := url.Parse(callbackURL); err != nil {
		return nil, fmt.Errorf("invalid callback URL: %w", err)
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
		callbackURL:  callbackURL,
		events: []TaskEvent{{
			TaskID:       taskID,
			OwnerID:      owner,
			State:        TaskStatePending,
			TaskQueue:    taskQueue,
			Timeout:      timeout,
			Retries:      0,
			MaxRetries:   maxRetries,
			StartTimeout: startTimeout,
			NotBefore:    notBefore,
		}},
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

	version := 0
	if versionStr, ok := data["version"]; ok {
		version, err = strconv.Atoi(versionStr)
		if err != nil {
			return nil, err
		}
	}

	callbackURL := ""
	if url, ok := data["callback_url"]; ok {
		callbackURL = url
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
		version:      version,
		callbackURL:  callbackURL,
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
	return t.state
}

func (t *Task) changeState(newState TaskState) {
	if newState == t.state {
		return
	}

	t.state = newState
	t.updated()

	t.events = append(t.events, TaskEvent{
		TaskID:       t.TaskID,
		OwnerID:      t.owner,
		State:        t.state,
		Version:      t.version,
		TaskQueue:    t.taskQueue,
		Timeout:      t.timeout,
		Retries:      t.retries,
		MaxRetries:   t.maxRetries,
		StartTimeout: t.startTimeout,
		NotBefore:    t.notBefore,
	})
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

func (t *Task) CollectEvents() []TaskEvent {
	events := t.events
	t.events = nil
	return events
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

func (t Task) Version() int {
	return t.version
}

func (t Task) CallbackURL() string {
	return t.callbackURL
}

func (t *Task) updated() {
	t.updatedAt = time.Now().Unix()
	t.version++
}

func (t *Task) retry() {
	t.retries++
	t.changeState(TaskStatePending)
}
