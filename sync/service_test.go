package sync

import (
	_ "github.com/alexbrainman/odbc"
	_ "github.com/go-sql-driver/mysql"
	//	_ "github.com/mattn/go-oci8"
	"github.com/stretchr/testify/assert"
	_ "github.com/viant/bgc"
	"github.com/viant/dsc"
	"log"
	"testing"
)

func TestService_Sync(t *testing.T) {

	//os.Setenv("LD_LIBRARY_PATH", "/opt/oracle/instantclient_12_2")
	//os.Setenv("PKG_CONFIG_PATH", "/opt/oracle/instantclient_12_2")

	dsc.Logf = dsc.StdoutLogger
	requestURL := ""
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
