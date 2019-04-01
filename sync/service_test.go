package sync

import (
	"github.com/viant/dsc"
	"log"
	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
	"os"
	"path"
	"testing"
	_ "github.com/mattn/go-oci8"
	_ "github.com/alexbrainman/odbc"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/viant/bgc"
)

func init() {
	os.Setenv("LD_LIBRARY_PATH", "/Users/awitas/Downloads/instantclient_12_2")
	os.Setenv("PKG_CONFIG_PATH", "/Users/awitas/Downloads/instantclient_12_2")

}



func TestService_Sync(t *testing.T) {

	dsc.Logf = dsc.StdoutLogger
	parent := toolbox.CallerDirectory(3)
	requestURL := path.Join(parent, "test/req.yaml")
	request, err := NewSyncRequestFromURL(requestURL)
	if !assert.Nil(t, err) {
		log.Fatal(err)
	}
	service := New()
	response := service.Sync(request)
	assert.EqualValues(t, "", response.Error)
}
