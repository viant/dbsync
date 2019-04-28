package sync

import (
	_ "github.com/alexbrainman/odbc"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-oci8"
	_ "github.com/viant/bgc"
	"os"
)

func init() {
	os.Setenv("LD_LIBRARY_PATH", "/Users/awitas/Downloads/instantclient_12_2")
	os.Setenv("PKG_CONFIG_PATH", "/Users/awitas/Downloads/instantclient_12_2")
}
