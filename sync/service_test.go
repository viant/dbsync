package sync

import (
	_ "github.com/alexbrainman/odbc"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-oci8"
	"github.com/stretchr/testify/assert"
	_ "github.com/viant/bgc"
	"github.com/viant/dsc"
	"log"
	"testing"
)



func 	TestService_Sync(t *testing.T) {
	//dsc.Logf = dsc.StdoutLogger
	requestURL := "/Projects/go/workspace/src/github.vianttech.com/adelphic/dbsync/vertica2bq/ci_dm/campaign_performance_hour_agg1.yaml"
	request, err := NewSyncRequestFromURL(requestURL)
	if !assert.Nil(t, err) {
		log.Fatal(err)
	}
	dsc.Logf = dsc.StdoutLogger

	service, err := New(&Config{Debug: true})
	assert.Nil(t, err)
	response := service.Sync(request)
	assert.EqualValues(t, "", response.Error)
}
