package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCreation(t *testing.T) {
	task := NewTask("test", 10, 3)

	assert.NotNil(t, task)
	assert.GreaterOrEqual(t, len(task.TaskID), 1)
	assert.Equal(t, "test", task.taskQueue)
	assert.Equal(t, task.timeout, int32(10))
	assert.Equal(t, task.retries, int32(0))
	assert.Equal(t, task.maxRetries, int32(3))
	assert.Equal(t, task.state, "pending")
	assert.InDelta(t, time.Now().Unix(), task.createdAt, 5)
	assert.InDelta(t, time.Now().Unix(), task.updatedAt, 5)
}

func TestState(t *testing.T) {
	task := NewTask("test", 10, 3)

	assert.Equal(t, task.State(), "pending")
}
func TestRetries(t *testing.T) {
	task := NewTask("test", 10, 3)

	assert.Equal(t, task.Retries(), int32(0))
}

func TestSelect(t *testing.T) {
	task := NewTask("test", 10, 3)

	lastModificationTime := task.updatedAt
	time.Sleep(1 * time.Second)
	err := task.Select()

	assert.Nil(t, err)
	assert.Equal(t, "running", task.state)
	assert.NotEqual(t, lastModificationTime, task.updatedAt)
}
func TestFailRetry(t *testing.T) {
	task := NewTask("test", 10, 1)
	task.Select()

	err := task.Fail()
	assert.Nil(t, err)

	assert.Nil(t, err)
	assert.Equal(t, "pending", task.state)
	assert.Equal(t, int32(1), task.retries)
}
func TestFailed(t *testing.T) {
	task := NewTask("test", 10, 1)
	task.Select()
	task.Fail()
	task.Select()

	err := task.Fail()
	assert.Nil(t, err)

	assert.Nil(t, err)
	assert.Equal(t, "failed", task.state)
	assert.Equal(t, int32(1), task.retries)
}
func TestTimeoutRetry(t *testing.T) {
	task := NewTask("test", 10, 1)
	task.Select()

	err := task.Timeout()
	assert.Nil(t, err)

	assert.Nil(t, err)
	assert.Equal(t, "pending", task.state)
	assert.Equal(t, int32(1), task.retries)
}
func TestTimedout(t *testing.T) {
	task := NewTask("test", 10, 1)
	task.Select()
	task.Timeout()
	task.Select()

	err := task.Timeout()
	assert.Nil(t, err)

	assert.Nil(t, err)
	assert.Equal(t, "timedout", task.state)
	assert.Equal(t, int32(1), task.retries)
}
func TestComplete(t *testing.T) {
	task := NewTask("test", 10, 1)
	task.Select()

	err := task.Complete()
	assert.Nil(t, err)

	assert.Nil(t, err)
	assert.Equal(t, "completed", task.state)
	assert.Equal(t, int32(0), task.retries)
}

func testFailUnauthorized(t *testing.T) {
	task := NewTask("test", 10, 3)

	time.Sleep(1 * time.Second)
	err := task.Fail()

	assert.NotNil(t, err)
}
