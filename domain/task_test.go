package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCreation(t *testing.T) {
	task := NewTask("laurent", "test", "payload", 10, 3)

	assert.NotNil(t, task)
	assert.GreaterOrEqual(t, len(task.TaskID), 1)
	assert.Equal(t, "test", task.taskQueue)
	assert.Equal(t, task.timeout, int32(10))
	assert.Equal(t, task.retries, int32(0))
	assert.Equal(t, task.maxRetries, int32(3))
	assert.Equal(t, task.state, "pending")
	assert.InDelta(t, time.Now().Unix(), task.createdAt, 5)
	assert.InDelta(t, time.Now().Unix(), task.updatedAt, 5)
	assert.InDelta(t, time.Now().Unix(), task.notBefore, 5)
}

func TestState(t *testing.T) {
	task := NewTask("laurent", "test", "payload", 10, 3)

	assert.Equal(t, task.State(), "pending")
}

func TestStateTimedout(t *testing.T) {
	task := NewTask("laurent", "test", "payload", 0, 3)

	assert.Equal(t, task.State(), "timedout")
}
func TestRetries(t *testing.T) {
	task := NewTask("laurent", "test", "payload", 10, 3)

	assert.Equal(t, task.Retries(), int32(0))
}

func TestSelect(t *testing.T) {
	task := NewTask("laurent", "test", "payload", 10, 3)

	lastModificationTime := task.updatedAt
	time.Sleep(1 * time.Second)
	err := task.Select()

	assert.Nil(t, err)
	assert.Equal(t, "running", task.state)
	assert.NotEqual(t, lastModificationTime, task.updatedAt)
}
func TestFailRetry(t *testing.T) {
	task := NewTask("laurent", "test", "payload", 10, 1)
	task.Select()

	err := task.Fail()
	assert.Nil(t, err)

	assert.Nil(t, err)
	assert.Equal(t, "pending", task.state)
	assert.Equal(t, int32(1), task.retries)
}
func TestFailed(t *testing.T) {
	task := NewTask("laurent", "test", "payload", 10, 1)
	task.Select()
	task.Fail()
	task.Select()

	err := task.Fail()
	assert.Nil(t, err)

	assert.Nil(t, err)
	assert.Equal(t, "failed", task.state)
	assert.Equal(t, int32(1), task.retries)
}
func TestComplete(t *testing.T) {
	task := NewTask("laurent", "test", "payload", 10, 1)
	task.Select()

	err := task.Complete("this is a result")
	assert.Nil(t, err)

	assert.Nil(t, err)
	assert.Equal(t, "completed", task.state)
	assert.Equal(t, "this is a result", task.result)
	assert.Equal(t, int32(0), task.retries)
}
func TestResult(t *testing.T) {
	task := NewTask("laurent", "test", "payload", 10, 1)
	task.Select()

	err := task.Complete("this is a result")
	assert.Nil(t, err)

	result, err := task.Result()
	assert.Nil(t, err)
	assert.Equal(t, "this is a result", result)
}

func testFailUnauthorized(t *testing.T) {
	task := NewTask("laurent", "test", "payload", 10, 3)

	time.Sleep(1 * time.Second)
	err := task.Fail()

	assert.NotNil(t, err)
}

func TestCancelRunning(t *testing.T) {
	task := NewTask("laurent", "test", "payload", 10, 1)
	task.Select()

	err := task.Cancel()
	assert.Nil(t, err)

	assert.Nil(t, err)
	assert.Equal(t, "canceled", task.state)
	assert.Equal(t, int32(0), task.retries)
}

func TestCancelPending(t *testing.T) {
	task := NewTask("laurent", "test", "payload", 10, 1)

	err := task.Cancel()
	assert.Nil(t, err)

	assert.Nil(t, err)
	assert.Equal(t, "canceled", task.state)
	assert.Equal(t, int32(0), task.retries)
}

func TestCancelOther(t *testing.T) {
	task := NewTask("laurent", "test", "payload", 10, 0)
	task.Select()
	task.Fail()
	assert.Equal(t, "failed", task.state)

	err := task.Cancel()
	assert.NotNil(t, err)
}

func TestOwner(t *testing.T) {
	task := NewTask("laurent", "test", "payload", 10, 0)

	assert.Equal(t, "laurent", task.Owner())
}

func TestCreationFuture(t *testing.T) {
	task, err := NewFutureTask("laurent", "test", "payload", 10, 3, time.Now().Unix()+10)

	assert.Nil(t, err)
	assert.NotNil(t, task)
	assert.GreaterOrEqual(t, len(task.TaskID), 1)
	assert.Equal(t, "test", task.taskQueue)
	assert.Equal(t, task.timeout, int32(10))
	assert.Equal(t, task.retries, int32(0))
	assert.Equal(t, task.maxRetries, int32(3))
	assert.Equal(t, task.state, "pending")
	assert.InDelta(t, time.Now().Unix(), task.createdAt, 5)
	assert.InDelta(t, time.Now().Unix(), task.updatedAt, 5)
	assert.InDelta(t, time.Now().Unix()+10, task.notBefore, 5)
}

func TestCreationPast(t *testing.T) {
	task, err := NewFutureTask("laurent", "test", "payload", 10, 3, time.Now().Unix()-10)

	assert.NotNil(t, err)
	assert.Nil(t, task)
}
