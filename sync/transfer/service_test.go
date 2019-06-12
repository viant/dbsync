package transfer

import (
	"dbsync/sync/dao"
	"dbsync/sync/data"
	"dbsync/sync/model"
	"dbsync/sync/shared"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
	"github.com/viant/toolbox"
	"io/ioutil"
	"net/http"
	"path"
	"testing"
)

var testConfig *dsc.Config

func init() {
	parent := toolbox.CallerDirectory(3)
	testConfig = &dsc.Config{
		DriverName: "sqlite3",
		Descriptor: path.Join(parent, "test/db/mydb"),
		Parameters: map[string]interface{}{
			"dbname":"mydb",
		},
	}
	_ = testConfig.Init()
}

func TestService_NewRequest(t *testing.T) {

	parent := toolbox.CallerDirectory(3)
	if !dsunit.InitFromURL(t, path.Join(parent, "test", "config.yaml")) {
		return
	}

	var useCases = []struct {
		description string
		table       string
		suffix      string
		expect      interface{}
		hasError    bool
	}{
		{
			description: "request",
			table:       "events",
			suffix:      "_tmp",
			expect: `{
	"Dest": {
		"DriverName": "sqlite3",
		"MaxPoolSize": 1,
		"PoolSize": 1,
		"Table": "events_tmp"
	},
	"Mode": "insert",
	"Source": {
		"DriverName": "sqlite3",
		"MaxPoolSize": 1,
		"PoolSize": 1,
		"Query": "SELECT  id,\ntimestamp,\nevent_type,\nquantity,\nmodified\nFROM events t "
	}
}`,
		},

		{
			description: "request",
			table:       "events_abc",
			suffix:      "_tmp",
			hasError:    true,
		},
	}

	for _, useCase := range useCases {
		ctx := &shared.Context{}
		dbSync := &model.Sync{
			Source: &model.Resource{
				Config: testConfig,
			},
			Dest: &model.Resource{
				Config: testConfig,
			},
			Table: useCase.table,
		}
		err := dbSync.Init()
		assert.Nil(t, err)

		daoService := dao.New(dbSync)
		err = daoService.Init(ctx)

		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}
		if ! assert.Nil(t, err, useCase.description) {
			continue
		}
		srv := New(dbSync, daoService)
		err = srv.Init()
		assert.Nil(t, err, useCase.description)

		request := srv.NewRequest(ctx, &data.Transferable{
			Suffix: useCase.suffix,
		})
		if !assertly.AssertValues(t, useCase.expect, request, useCase.description) {
			_ = toolbox.DumpIndent(request, true)
		}
	}
}

func TestService_Post(t *testing.T) {

	responses := make(chan string, 10)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = ioutil.ReadAll(r.Body)
		_ = r.Body.Close()
		response := <-responses
		_, _ = fmt.Fprintf(w, response)
	})
	go http.ListenAndServe(":6081", nil)

	parent := toolbox.CallerDirectory(3)
	if !dsunit.InitFromURL(t, path.Join(parent, "test", "config.yaml")) {
		return
	}

	var useCases = []struct {
		description string
		table       string
		retries     int
		tempDB string
		suffix      string
		expect      interface{}
		hasError    bool
		expectTransferred int
		responses   []string
	}{
		{
			description: "request with done status",
			table:       "events",
			suffix:      "_tmp",

			retries:     3,
			expectTransferred:10,
			responses: []string{
				`{"status":"running"}`,
				`{"status":"error", "error":"test"}`,
				`{"Status":"error", "error":"test"}`,
				`{"Status":"done", "WriteCount":10}`,
			},
		},
		{
			description: "request with error",
			table:       "events",
			suffix:      "_tmp",
			retries:     2,
			hasError:    true,
			responses: []string{
				`{"status":"running"}`,
				`{"status":"error", "error":"test"}`,
				`{"Status":"error", "error":"test"}`,
			},
		},
		{
			description: "request with wait",
			table:       "events",
			suffix:      "_tmp",
			retries:     2,
			expectTransferred:5,
			responses: []string{
				`{"status":"running", "WriteCount":3}`,
				`{"status":"running", "WriteCount":4}`,
				`{"Status":"done", "WriteCount":5}`,
			},
		},
		{
			description: "request with tempdb",
			table:       "events",
			suffix:      "_tmp",
			retries:     2,
			tempDB:"temp",
			expectTransferred:6,
			responses: []string{
				`{"status":"running", "WriteCount":3}`,
				`{"status":"running", "WriteCount":4}`,
				`{"Status":"done", "WriteCount":6}`,
			},
		},
	}

	for _, useCase := range useCases {
		ctx := &shared.Context{Debug: false}
		dbSync := &model.Sync{
			Source: &model.Resource{
				Config: testConfig,
			},
			Dest: &model.Resource{
				Config: testConfig,
			},
			Table: useCase.table,
		}
		dbSync.Transfer.EndpointIP = "127.0.0.1:6081"
		dbSync.Transfer.MaxRetries = useCase.retries
		dbSync.Transfer.TempDatabase = useCase.tempDB
		err := dbSync.Init()
		assert.Nil(t, err, useCase.description)


		if len(useCase.responses) > 0 {
			for _, resp := range useCase.responses {
				responses <- resp
			}
		}

		daoService := dao.New(dbSync)
		err = daoService.Init(ctx)


		if ! assert.Nil(t, err, useCase.description) {
			continue
		}

		srv := New(dbSync, daoService)
		err = srv.Init()
		assert.Nil(t, err, useCase.description)

		transferable := &data.Transferable{
			Suffix: useCase.suffix,
		}
		request := srv.NewRequest(ctx, transferable)


		err = srv.Post(ctx, request, transferable)
		if useCase.hasError {
			assert.NotNil(t, err, useCase.description)
			continue
		}
		assert.Nil(t, err, useCase.description)
		assert.EqualValues(t, useCase.expectTransferred, transferable.Transferred, useCase.description)
	}
}
