package transfer

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"testing"
	"time"
)

func TestService_Transfer(t *testing.T) {

	_ = toolbox.RemoveFileIfExist("test/transfer/test_users.json")
	config, err := dsc.NewConfigFromURL("test/config.yaml")
	if !assert.Nil(t, err) {
		return
	}
	request := &Request{
		Source: &Source{
			Config: config,
			Query:  "SELECT id, name, email, address.state AS state FROM users",
		},
		Dest: &Dest{
			Config: config,
			Table:  "test_users",
		},
	}
	service := New(nil)
	assert.NotNil(t, service)
	response := service.Transfer(request)
	assert.NotNil(t, response)
	assert.Equal(t, "done", response.Status)
	for {
		taskStatus := service.Task(response.TaskID, nil)
		if taskStatus.Status == "running" {
			time.Sleep(time.Second)
			continue
		}
		assert.Equal(t, "done", taskStatus.Status)
		assert.Equal(t, "", taskStatus.Error)
		assert.Equal(t, uint64(3), taskStatus.WriteCount)
		assert.Equal(t, 3, taskStatus.ReadCount)
		break
	}
}
