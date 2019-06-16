package dao

import (
	"dbsync/sync/core"
	"dbsync/sync/criteria"
	"dbsync/sync/model"
	"dbsync/sync/shared"
	"dbsync/sync/sql"
	"errors"
	"fmt"
	"github.com/viant/dsc"
	"strings"
)

//SuffixWasEmptyErr represents suffix empty error
var SuffixWasEmptyErr = errors.New("suffix was empty")

type Service interface {
	Partitions(ctx *shared.Context, kind model.ResourceKind) (core.Records, error)

	Signatures(ctx *shared.Context, kind model.ResourceKind, filter map[string]interface{}) (core.Records, error)

	Signature(ctx *shared.Context, kind model.ResourceKind, filter map[string]interface{}) (core.Record, error)

	CountSignature(ctx *shared.Context, kind model.ResourceKind, filter map[string]interface{}) (*core.Signature, error)

	ChunkSignature(ctx *shared.Context, kind model.ResourceKind, offset, limit int, filter map[string]interface{}) (*core.Signature, error)

	ExecSQL(ctx *shared.Context, SQL string) error

	Columns(ctx *shared.Context, table string) ([]dsc.Column, error)

	DbName(ctx *shared.Context, kind model.ResourceKind) (string, error)

	CreateTransientTable(ctx *shared.Context, suffix string) error

	DropTransientTable(ctx *shared.Context, suffix string) error

	RecreateTransientTable(ctx *shared.Context, suffix string) error

	Builder() *sql.Builder

	Init(ctx *shared.Context) error

	Close() error
}

type dbResource struct {
	*model.Resource
	DB dsc.Manager
}

type service struct {
	*model.Sync
	source  *dbResource
	dest    *dbResource
	builder *sql.Builder
}

func (s *service) dbResource(kind model.ResourceKind) *dbResource {
	if kind == model.ResourceKindDest {
		return s.dest
	}
	return s.source
}

func (s *service) Builder() *sql.Builder {
	return s.builder
}

func (s *service) Partitions(ctx *shared.Context, kind model.ResourceKind) (core.Records, error) {
	dbResource := s.dbResource(kind)
	return s.partitions(ctx, dbResource)

}

func (s *service) DropTransientTable(ctx *shared.Context,suffix string) (err error) {
	if suffix == "" {
		return SuffixWasEmptyErr
	}
	table := s.builder.Table(suffix)
	dbName := s.Transfer.TempDatabase
	if dbName == "" {
		if dbName, err = s.DbName(ctx, model.ResourceKindDest); err != nil {
			return err
		}
	}
	ctx.Log(fmt.Sprintf("DROP TABLE %v\n", table))
	dialect := dsc.GetDatastoreDialect(s.dest.DB.Config().DriverName)
	return dialect.DropTable(s.dest.DB, dbName, table)

}

func (s *service) RecreateTransientTable(ctx *shared.Context, suffix string) (err error) {
	_ = s.DropTransientTable(ctx, suffix)
	return s.CreateTransientTable(ctx, suffix)
}

func (s *service) CreateTransientTable(ctx *shared.Context, suffix string) (err error) {
	if suffix == "" {
		return SuffixWasEmptyErr
	}
	dbName := s.Transfer.TempDatabase
	if dbName == "" {
		if dbName, err = s.DbName(ctx, model.ResourceKindDest); err != nil {
			return err
		}
	}
	DDL := s.builder.DDLFromSelect(suffix)
	if err = s.ExecSQL(ctx, DDL); err == nil {
		return nil
	}
	//Fallback to dialect DDL
	DDL = s.builder.DDL(suffix)
	if s.Transfer.TempDatabase != "" {
		DDL = strings.Replace(DDL, dbName+".", "", 1)
	}
	return s.ExecSQL(ctx, DDL)
}

func (s *service) partitions(ctx *shared.Context, resource *dbResource) (core.Records, error) {
	result := core.Records{}
	ctx.Log(resource.PartitionSQL)
	if resource.PartitionSQL == "" {
		return nil, fmt.Errorf("partitionSQL was empty")
	}
	err := resource.DB.ReadAll(&result, resource.PartitionSQL, nil, nil)
	return result, err
}

func (s *service) Signatures(ctx *shared.Context,kind model.ResourceKind, filter map[string]interface{}) (core.Records, error) {
	dbResource := s.dbResource(kind)
	return s.signatures(ctx, dbResource, filter)
}

func (s *service) Signature(ctx *shared.Context, kind model.ResourceKind, filter map[string]interface{}) (core.Record, error) {
	result, err := s.Signatures(ctx, kind, filter)
	if err != nil {
		return nil, err
	}
	if len(result) == 1 {
		return result[0], nil
	} else if len(result) > 1 {
		return nil, fmt.Errorf("expected one record, but had: %v", len(result))
	}
	return nil, nil
}

func (s *service) signatures(ctx *shared.Context, dbResource *dbResource, filter map[string]interface{}) (core.Records, error) {
	result := core.Records{}
	SQL := s.builder.SignatureDQL(dbResource.Resource, filter)
	ctx.Log(SQL)
	err := dbResource.DB.ReadAll(&result, SQL, nil, nil)
	return result, err
}

func (s *service) CountSignature(ctx *shared.Context,kind model.ResourceKind, filter map[string]interface{}) (*core.Signature, error) {
	dbResource := s.dbResource(kind)
	return s.countSignature(ctx, dbResource, filter)
}

func (s *service) countSignature(ctx *shared.Context, dbResource *dbResource, filter map[string]interface{}) (*core.Signature, error) {
	result := &core.Signature{}
	SQL := s.builder.CountDQL("", dbResource.Resource, filter)
	ctx.Log(SQL)
	ok, err := dbResource.DB.ReadSingle(result, SQL, nil, nil)
	if ! ok {
		return nil, err
	}
	return result, err
}

func (s *service) ChunkSignature(ctx *shared.Context,kind model.ResourceKind, offset, limit int, filter map[string]interface{}) (*core.Signature, error) {
	dbResource := s.dbResource(kind)
	return s.chunkSignature(ctx, dbResource, offset, limit, filter)
}

func (s *service) chunkSignature(ctx *shared.Context, dbResource *dbResource, offset, limit int, filter map[string]interface{}) (*core.Signature, error) {
	result := &core.Signature{}
	if len(filter) == 0 {
		filter = map[string]interface{}{}
	}
	filter[s.Sync.IDColumns[0]] = criteria.NewGraterOrEqual(offset)
	SQL := s.builder.ChunkDQL(dbResource.Resource, offset, limit, filter)
	ctx.Log(SQL)
	ok, err := dbResource.DB.ReadSingle(result, SQL, nil, nil)
	if ! ok {
		return nil, err
	}
	return result, err
}

func (s *service) ExecSQL(ctx *shared.Context,SQL string) error {
	ctx.Log(SQL)
	_, err := s.dest.DB.Execute(SQL)
	return err
}

func (s *service) DbName(ctx *shared.Context,kind model.ResourceKind) (string, error) {
	dbResource := s.dbResource(kind)
	dialect := dsc.GetDatastoreDialect(dbResource.DB.Config().DriverName)
	return dialect.GetCurrentDatastore(dbResource.DB)
}

func (s *service) Columns(ctx *shared.Context, table string) ([]dsc.Column, error) {
	dialect := dsc.GetDatastoreDialect(s.dest.DB.Config().DriverName)
	datastore, err := dialect.GetCurrentDatastore(s.dest.DB)
	if err != nil {
		return nil, err
	}
	return dialect.GetColumns(s.dest.DB, datastore, table)
}

func (s *service) Close() error {
	_ = s.dest.DB.ConnectionProvider().Close()
	return s.source.DB.ConnectionProvider().Close()
}

func (s *service) initDB(ctx *shared.Context) (err error) {
	if s.dest.DB, err = dsc.NewManagerFactory().Create(s.dest.Config); err == nil {
		s.source.DB, err = dsc.NewManagerFactory().Create(s.source.Config)
	}
	return err
}

func (s *service) initBuilder(ctx *shared.Context) error {
	columns, err := s.Columns(ctx, s.dest.Table)
	if err != nil {
		return err
	}
	dialect := dsc.GetDatastoreDialect(s.dest.DB.Config().DriverName)
	DDL, _ := dialect.ShowCreateTable(s.dest.DB, s.dest.Table)
	s.builder, err = sql.NewBuilder(s.Sync, DDL, columns)
	return err
}

func (s *service) Init(ctx *shared.Context) error {
	err := s.initDB(ctx)
	if err == nil {
		err = s.initBuilder(ctx)
	}
	return err
}

//New returns new service
func New(sync *model.Sync) *service {

	return &service{
		Sync:   sync,
		source: &dbResource{Resource: sync.Source},
		dest:   &dbResource{Resource: sync.Dest},
	}
}
