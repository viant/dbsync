package sync

import (
	_ "github.com/alexbrainman/odbc"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-oci8"
	"github.com/stretchr/testify/assert"
	_ "github.com/viant/bgc"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"log"
	"path"
	"testing"
)

func TestService_Sync(t *testing.T) {

	dsc.Logf = dsc.StdoutLogger
	parent := toolbox.CallerDirectory(3)
	requestURL := path.Join(parent, "test/site_list_entry.yaml")
	request, err := NewSyncRequestFromURL(requestURL)
	if !assert.Nil(t, err) {
		log.Fatal(err)
	}
	service, err := New(&Config{Debug: true})
	assert.Nil(t, err)
	response := service.Sync(request)
	assert.EqualValues(t, "", response.Error)
}
