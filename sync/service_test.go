package sync

import (
	"dbsync/sync/history"
	"dbsync/sync/shared"
	"dbsync/transfer"
	"fmt"
	_ "github.com/alexbrainman/odbc"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/assertly"
	"github.com/viant/dsunit"
	"github.com/viant/toolbox"
	"path"

	//_ "github.com/mattn/go-oci8"
	"github.com/stretchr/testify/assert"
	_ "github.com/viant/bgc"
	"log"
	"testing"
)

var transferServer transfer.Server

func init() {
	service := transfer.New(nil)
	server := transfer.NewServer(service, 8080)
	go func() { _ = server.ListenAndServe() }()
}

func TestService_Sync(t *testing.T) {

	parent := toolbox.CallerDirectory(3)
	service, err := New(&shared.Config{Debug: true, MaxHistory: 1})
	assert.Nil(t, err)

	var useCases = []struct {
		description    string
		caseURI        string
		expectResponse interface{}
		expectJob      interface{}
	}{

		{
			description: "partition with id base",
			caseURI:     "id_based",
			expectResponse: `{
	"DestCount": 5,
	"JobID": "events2",
	"SourceCount": 7,
	"Status": "done",
	"Transferred": 7
}`,
			expectJob: `{
	"DestCount": 5,
	"ID": "events2",
	"Partitions": {
		"deleteMerge": 1,
		"inSync": 1,
		"insert": 1,
		"merge": 1
	},
	"SourceCount": 7,
	"Status": "done",
	"Transferred": 7
}
`,
		},
	}

	for _, useCase := range useCases {

		if !dsunit.InitFromURL(t, path.Join(parent, "test", "config.yaml")) {
			return
		}
		initDataset := dsunit.NewDatasetResource("db1", path.Join(parent, fmt.Sprintf("test/case/%v/prepare", useCase.caseURI)), "", "")
		if !dsunit.Prepare(t, dsunit.NewPrepareRequest(initDataset)) {
			return
		}

		request, err := NewRequestFromURL(path.Join(parent, fmt.Sprintf("test/case/%v/request.yaml", useCase.caseURI)))
		if !assert.Nil(t, err) {
			log.Fatal(err)
		}

		response := service.Sync(request)
		if !assertly.AssertValues(t, useCase.expectResponse, response, useCase.description) {
			_ = toolbox.DumpIndent(response, true)
		}
		historyResponse := service.History().Show(&history.ShowRequest{ID: "events2"})
		if assert.EqualValues(t, 1, len(historyResponse.Items)) {
			if !assertly.AssertValues(t, useCase.expectJob, historyResponse.Items[0], useCase.description) {
				_ = toolbox.DumpIndent(historyResponse.Items[0], true)
			}
		}
		expectDataset := dsunit.NewDatasetResource("db1", path.Join(parent, fmt.Sprintf("test/case/%v/expect", useCase.caseURI)), "", "")
		if !dsunit.Prepare(t, dsunit.NewPrepareRequest(expectDataset)) {
			return
		}
	}
}
