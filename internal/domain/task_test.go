package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCreation(t *testing.T) {
	task, err := NewTask("laurent", "test", "payload", 10, 0, 3, "http://localhost:8080/callback")
	assert.NoError(t, err)
	assert.NotNil(t, task)
	assert.GreaterOrEqual(t, len(task.TaskID), 1)
	assert.Equal(t, "test", task.taskQueue)
	assert.Equal(t, task.timeout, int32(10))
	assert.Equal(t, task.startTimeout, int32(0))
	assert.Equal(t, task.retries, int32(0))
	assert.Equal(t, task.maxRetries, int32(3))
	assert.Equal(t, task.state, TaskStatePending)
	assert.InDelta(t, time.Now().Unix(), task.createdAt, 5)
	assert.InDelta(t, time.Now().Unix(), task.updatedAt, 5)
	assert.InDelta(t, time.Now().Unix(), task.notBefore, 5)
	assert.Equal(t, "http://localhost:8080/callback", task.CallbackURL())
}

func TestNewTaskEmptyCallbackURL(t *testing.T) {
	task, err := NewTask(
		"laurent",
		"test",
		"payload",
		10, 0, 3,
		"",
	)
	assert.NoError(t, err)
	assert.Empty(t, task.CallbackURL())
}

func TestState(t *testing.T) {
	task, err := NewTask("laurent", "test", "payload", 10, 0, 3, "")
	assert.NoError(t, err)

	assert.Equal(t, task.State(), TaskStatePending)
}

func TestRetries(t *testing.T) {
	task, err := NewTask("laurent", "test", "payload", 10, 0, 3, "")
	assert.NoError(t, err)

	assert.Equal(t, task.Retries(), int32(0))
}

func TestSelect(t *testing.T) {
	task, err := NewTask("laurent", "test", "payload", 10, 0, 3, "")
	assert.NoError(t, err)

	lastModificationTime := task.updatedAt
	time.Sleep(1 * time.Second)
	err = task.Select()

	assert.Nil(t, err)
	assert.Equal(t, TaskStateRunning, task.state)
	assert.NotEqual(t, lastModificationTime, task.updatedAt)
}

func TestFailRetry(t *testing.T) {
	task, err := NewTask("laurent", "test", "payload", 10, 0, 1, "")
	assert.NoError(t, err)

	err = task.Select()
	assert.NoError(t, err)

	err = task.Fail()
	assert.Nil(t, err)

	assert.Nil(t, err)
	assert.Equal(t, TaskStatePending, task.state)
	assert.Equal(t, int32(1), task.retries)
}

func TestFailed(t *testing.T) {
	task, err := NewTask("laurent", "test", "payload", 10, 0, 1, "")
	assert.NoError(t, err)

	err = task.Select()
	assert.NoError(t, err)

	err = task.Fail()
	assert.NoError(t, err)

	err = task.Select()
	assert.NoError(t, err)

	err = task.Fail()
	assert.Nil(t, err)

	assert.Equal(t, TaskStateFailed, task.state)
	assert.Equal(t, int32(1), task.retries)
}

func TestTimeoutRetry(t *testing.T) {
	task, err := NewTask("laurent", "test", "payload", 10, 0, 1, "")
	assert.NoError(t, err)

	err = task.Select()
	assert.NoError(t, err)

	err = task.Timeout()
	assert.Nil(t, err)

	assert.Equal(t, TaskStatePending, task.state)
	assert.Equal(t, int32(1), task.retries)
}

func TestTimedout(t *testing.T) {
	task, err := NewTask("laurent", "test", "payload", 10, 0, 1, "")
	assert.NoError(t, err)

	err = task.Select()
	assert.NoError(t, err)

	err = task.Timeout()
	assert.NoError(t, err)

	err = task.Select()
	assert.NoError(t, err)

	err = task.Timeout()
	assert.Nil(t, err)

	assert.Equal(t, TaskStateTimedout, task.state)
	assert.Equal(t, int32(1), task.retries)
}

func TestComplete(t *testing.T) {
	task, err := NewTask("laurent", "test", "payload", 10, 0, 1, "")
	assert.NoError(t, err)

	err = task.Select()
	assert.NoError(t, err)

	err = task.Complete("this is a result")
	assert.Nil(t, err)

	assert.Nil(t, err)
	assert.Equal(t, TaskStateCompleted, task.state)
	assert.Equal(t, "this is a result", task.result)
	assert.Equal(t, int32(0), task.retries)
}

func TestResult(t *testing.T) {
	task, err := NewTask("laurent", "test", "payload", 10, 0, 1, "")
	assert.NoError(t, err)

	err = task.Select()
	assert.NoError(t, err)

	err = task.Complete("this is a result")
	assert.Nil(t, err)

	result, err := task.Result()
	assert.Nil(t, err)
	assert.Equal(t, "this is a result", result)
}

func TestFailNotRunning(t *testing.T) {
	task, err := NewTask("laurent", "test", "payload", 10, 0, 3, "")
	assert.NoError(t, err)

	err = task.Fail()
	assert.Error(t, err)
}

func TestCancelRunning(t *testing.T) {
	task, err := NewTask("laurent", "test", "payload", 10, 0, 1, "")
	assert.NoError(t, err)

	err = task.Select()
	assert.NoError(t, err)

	err = task.Cancel()
	assert.Nil(t, err)

	assert.Nil(t, err)
	assert.Equal(t, TaskStateCanceled, task.state)
	assert.Equal(t, int32(0), task.retries)
}

func TestCancelPending(t *testing.T) {
	task, err := NewTask("laurent", "test", "payload", 10, 0, 1, "")
	assert.NoError(t, err)

	err = task.Cancel()
	assert.NoError(t, err)

	assert.Equal(t, TaskStateCanceled, task.state)
	assert.Equal(t, int32(0), task.retries)
}

func TestCancelOther(t *testing.T) {
	task, err := NewTask("laurent", "test", "payload", 10, 0, 0, "")
	assert.NoError(t, err)

	err = task.Select()
	assert.NoError(t, err)

	err = task.Fail()
	assert.NoError(t, err)
	assert.Equal(t, TaskStateFailed, task.state)

	err = task.Cancel()
	assert.NotNil(t, err)
}

func TestOwner(t *testing.T) {
	task, err := NewTask("laurent", "test", "payload", 10, 0, 0, "")
	assert.NoError(t, err)

	assert.Equal(t, "laurent", task.Owner())
}

func TestCreationFuture(t *testing.T) {
	task, err := NewFutureTask("laurent", "test", "payload", 10, 0, 3, time.Now().Unix()+10, "")

	assert.Nil(t, err)
	assert.NotNil(t, task)
	assert.GreaterOrEqual(t, len(task.TaskID), 1)
	assert.Equal(t, "test", task.taskQueue)
	assert.Equal(t, task.timeout, int32(10))
	assert.Equal(t, task.startTimeout, int32(0))
	assert.Equal(t, task.retries, int32(0))
	assert.Equal(t, task.maxRetries, int32(3))
	assert.Equal(t, task.state, TaskStatePending)
	assert.InDelta(t, time.Now().Unix(), task.createdAt, 5)
	assert.InDelta(t, time.Now().Unix(), task.updatedAt, 5)
	assert.InDelta(t, time.Now().Unix()+10, task.notBefore, 5)
}

func TestCreationPast(t *testing.T) {
	task, err := NewFutureTask("laurent", "test", "payload", 10, 0, 3, time.Now().Unix()-10, "")

	assert.NotNil(t, err)
	assert.Nil(t, task)
}

func TestCreationStartTimeout(t *testing.T) {
	task, err := NewFutureTask("laurent", "test", "payload", 10, 7, 3, time.Now().Unix()+10, "")

	assert.Nil(t, err)
	assert.NotNil(t, task)
	assert.GreaterOrEqual(t, len(task.TaskID), 1)
	assert.Equal(t, "test", task.taskQueue)
	assert.Equal(t, task.timeout, int32(10))
	assert.Equal(t, task.startTimeout, int32(7))
	assert.Equal(t, task.retries, int32(0))
	assert.Equal(t, task.maxRetries, int32(3))
	assert.Equal(t, task.state, TaskStatePending)
	assert.InDelta(t, time.Now().Unix(), task.createdAt, 5)
	assert.InDelta(t, time.Now().Unix(), task.updatedAt, 5)
	assert.InDelta(t, time.Now().Unix()+10, task.notBefore, 5)
}

func TestTaskFromStringMap(t *testing.T) {
	task, err := TaskFromStringMap(map[string]string{
		"task_id":      "Such Task",
		"owner":        "my owner",
		"payload":      "content",
		"task_queue":   "queue",
		"state":        string(TaskStatePending),
		"timeout":      "30",
		"startTimeout": "7",
		"retries":      "1",
		"maxRetries":   "3",
		"created_at":   "1000",
		"updated_at":   "2000",
		"not_before":   "1000",
		"version":      "42",
		"callback_url": "http://localhost:8080/callback",
	})

	assert.NotNil(t, task)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(task.TaskID), 1)
	assert.Equal(t, "queue", task.taskQueue)
	assert.Equal(t, task.timeout, int32(30))
	assert.Equal(t, task.startTimeout, int32(7))
	assert.Equal(t, task.retries, int32(1))
	assert.Equal(t, task.maxRetries, int32(3))
	assert.Equal(t, task.state, TaskStatePending)
	assert.Equal(t, task.createdAt, int64(1000))
	assert.Equal(t, task.updatedAt, int64(2000))
	assert.Equal(t, task.notBefore, int64(1000))
	assert.Equal(t, task.version, 42)
	assert.Equal(t, task.version, 42)
	assert.Equal(t, task.callbackURL, "http://localhost:8080/callback")
}

func TestNewInvalidCallbackURL(t *testing.T) {
	task, err := NewTask(
		"laurent",
		"test",
		"payload",
		10, 0, 3,
		"foo\t",
	)
	assert.Error(t, err)
	assert.Nil(t, task)
}
