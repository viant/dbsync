package main


import (
	"github.com/viant/dbsync/sync"
	"github.com/viant/dsc"
	"log"
	"github.com/viant/toolbox"
	"os"
	"path"
	_ "github.com/mattn/go-oci8"
	_ "github.com/alexbrainman/odbc"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/viant/bgc"
)

func init() {
	os.Setenv("LD_LIBRARY_PATH", "/Users/awitas/Downloads/instantclient_12_2")
	os.Setenv("PKG_CONFIG_PATH", "/Users/awitas/Downloads/instantclient_12_2")

}





func main()  {
		dsc.Logf = dsc.StdoutLogger
		parent := toolbox.CallerDirectory(3)
		requestURL := path.Join(parent, "req_site_new.yaml")
		request, err := sync.NewSyncRequestFromURL(requestURL)
		if err != nil {
			log.Fatal(err)
		}
		service := sync.New()
		service.Sync(request)

}
