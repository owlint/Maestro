# Maestro

Maestro is a Task Scheduler written in Golang and supported by Owlint. 

The main features of Maestro are :

* Agnostic of the client and worker language
* Timeout
* Retries
* Cancel
* Cron Tasks

## Roadmap
Maestro is at a very early stage of development, not really usable and thus **NOT PRODUCTION READY**.

### v0.1.0:  Actual

* Tasks can be created (**without** payload)
* Tasks can be got by workers from queue
* Workers can change complete (**without** result), fail or timeout tasks
* Retries are working
* Timeout is not yet working
* Cron tasks are not yet implemented
* Cancel is not yet implemented

### v1.0.0

* Tasks can be created (**with** payload)
* Tasks can be got by workers from queue
* Workers can change complete (**with** result), fail or timeout tasks
* Retries are working
* Timeout is **working**
* Cron tasks are not yet implemented
* Cancel is **working**

### v1.1.0

* Tasks can be created (with payload)
* Tasks can be got by workers from queue
* Workers can change complete (with result), fail or timeout tasks
* Retries are working
* Timeout is working
* Cron tasks are **working**
* Cancel is working

## Run

```bash
make docker.recreate
go run cmd/srv/main.go
```

Service will be bind to port 8080.

### Create task

```bash
curl --request POST \
  --url http://localhost:8080/api/task/create \
  --header 'Content-Type: application/json' \
  --data '{
	"queue": "queueName",
	"retries": 3,
	"timeout": 300
}'
```

```json
{
  "task_id": "Task-74484551-70c0-41a1-a9f0-db36923379e2"
}
```

### Task State

```bash
curl --request POST \
  --url http://localhost:8080/api/task/get \
  --header 'Content-Type: application/json' \
  --data '{
	"task_id": "Task-74484551-70c0-41a1-a9f0-db36923379e2"
}'
```

```json
{
  "TaskID": "Task-74484551-70c0-41a1-a9f0-db36923379e2",
  "Queue": "queueName",
  "State": "pending",
  "LastUpdate": 1614350994
}
```

### Next Task in queue

```bash
curl --request POST \
  --url http://localhost:8080/api/queue/next \
  --header 'Content-Type: application/json' \
  --data '{
	"queue": "queueName"
}'
```

```json
{
  "TaskID": "Task-74484551-70c0-41a1-a9f0-db36923379e2",
  "Queue": "queueName",
  "State": "pending",
  "LastUpdate": 1614350994
}
```
### Complete task

```bash
curl --request POST \
  --url http://localhost:8080/api/task/complete \
  --header 'Content-Type: application/json' \
  --data '{
	"task_id": "Task-6b6fdc56-d9da-477f-b9ea-86f3493bae42"
}'
```

### Fail task

```bash
curl --request POST \
  --url http://localhost:8080/api/task/fail \
  --header 'Content-Type: application/json' \
  --data '{
	"task_id": "Task-6b6fdc56-d9da-477f-b9ea-86f3493bae42"
}'
```

### Timeout task

```bash
curl --request POST \
  --url http://localhost:8080/api/task/timeout \
  --header 'Content-Type: application/json' \
  --data '{
	"task_id": "Task-6b6fdc56-d9da-477f-b9ea-86f3493bae42"
}'
```
