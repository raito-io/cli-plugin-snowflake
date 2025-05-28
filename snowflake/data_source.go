package snowflake

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/smithy-go/ptr"
	"github.com/gammazero/workerpool"
	"github.com/hashicorp/go-multierror"
	ds "github.com/raito-io/cli/base/data_source"
	"github.com/raito-io/cli/base/tag"
	"github.com/raito-io/cli/base/wrappers"
	"github.com/raito-io/golang-set/set"

	"github.com/raito-io/cli-plugin-snowflake/common"
)

const AccountAdmin = "ACCOUNTADMIN"

//go:generate go run github.com/vektra/mockery/v2 --name=dataSourceRepository --with-expecter --inpackage
type dataSourceRepository interface {
	Close() error
	TotalQueryTime() time.Duration
	GetSnowFlakeAccountName(ops ...func(options *GetSnowFlakeAccountNameOptions)) (string, error)
	GetWarehouses() ([]DbEntity, error)
	GetInboundShares() ([]DbEntity, error)
	GetDatabases() ([]DbEntity, error)
	GetSchemasInDatabase(databaseName string, handleEntity EntityHandler) error
	GetFunctionsInDatabase(databaseName string, handleEntity EntityHandler) error
	GetProceduresInDatabase(databaseName string, handleEntity EntityHandler) error
	GetTablesInDatabase(databaseName string, schemaName string, handleEntity EntityHandler) error
	GetColumnsInDatabase(databaseName string, handleEntity EntityHandler) error
	GetTagsLinkedToDatabaseName(databaseName string) (map[string][]*tag.Tag, error)
	GetTagsByDomain(domain string) (map[string][]*tag.Tag, error)
	ExecuteGrantOnAccountRole(perm, on, role string, isSystemGrant bool) error
	GetIntegrations() ([]DbEntity, error)
	GetApplications() ([]ApplictionEntity, error)
}

type DataSourceSyncer struct {
	repoProvider func(params map[string]string, role string) (dataSourceRepository, error)
	SfSyncRole   string

	startFrom         string
	excludeChildren   []string
	skipColumns       bool
	schemaExcludes    set.Set[string]
	inboundSharesMap  set.Set[string]
	repo              dataSourceRepository
	dataSourceHandler wrappers.DataSourceObjectHandler
	lock              sync.Mutex
}

func NewDataSourceSyncer() *DataSourceSyncer {
	return &DataSourceSyncer{
		repoProvider: newDataSourceSnowflakeRepo,
	}
}

func newDataSourceSnowflakeRepo(params map[string]string, role string) (dataSourceRepository, error) {
	return NewSnowflakeRepository(params, role)
}

// shouldHandle determines if this data object needs to be handled by the syncer or not. It does this by looking at the configuration options to only sync a part.
func (s *DataSourceSyncer) shouldHandle(fullName string) bool {
	// No partial sync specified, so do everything
	if s.startFrom == "" {
		return true
	}

	// Check if the data object is under the data object to start from
	if !strings.HasPrefix(fullName, s.startFrom) || s.startFrom == fullName {
		return false
	}

	// Check if we hit any excludes
	for _, exclude := range s.excludeChildren {
		if strings.HasPrefix(fullName, s.startFrom+"."+exclude) {
			return false
		}
	}

	return true
}

// shouldGoInto checks if we need to go deeper into this data object or not.
func (s *DataSourceSyncer) shouldGoInto(fullName string) bool {
	// No partial sync specified, so do everything
	if s.startFrom == "" || strings.HasPrefix(s.startFrom, fullName) || strings.HasPrefix(fullName, s.startFrom) {
		return true
	}

	return false
}

func (s *DataSourceSyncer) SyncDataSource(ctx context.Context, dataSourceHandler wrappers.DataSourceObjectHandler, config *ds.DataSourceSyncConfig) error {
	// Initializing parameters
	configParams := config.ConfigMap
	s.dataSourceHandler = dataSourceHandler
	s.startFrom = config.DataObjectParent
	s.excludeChildren = config.DataObjectExcludes
	s.skipColumns = configParams.GetBoolWithDefault(SfSkipColumns, false)
	s.SfSyncRole = configParams.GetStringWithDefault(SfRole, AccountAdmin)

	excludedDatabases := "SNOWFLAKE"
	if v, ok := configParams.Parameters[SfExcludedDatabases]; ok {
		excludedDatabases = v
	}

	dbExcludes := parseCommaSeparatedList(excludedDatabases)

	excludedSchemaList := "INFORMATION_SCHEMA"
	if v, ok := configParams.Parameters[SfExcludedSchemas]; ok {
		excludedSchemaList += "," + v
	}

	s.schemaExcludes = parseCommaSeparatedList(excludedSchemaList)
	standard := configParams.GetBoolWithDefault(SfStandardEdition, false)
	skipTags := configParams.GetBoolWithDefault(SfSkipTags, false)
	shouldRetrieveTags := !standard && !skipTags

	repo, err := s.repoProvider(configParams.Parameters, "")
	if err != nil {
		return err
	}

	defer func() {
		Logger.Info(fmt.Sprintf("Total snowflake query time:  %s", repo.TotalQueryTime()))
		repo.Close()
	}()

	s.repo = repo

	// for data source level access import & export convenience we retrieve the snowflake account and use it as datasource name
	sfAccount, err := repo.GetSnowFlakeAccountName()
	if err != nil {
		return err
	}

	// Initializing the data source handler
	dataSourceHandler.SetDataSourceName(sfAccount)
	dataSourceHandler.SetDataSourceFullname(sfAccount)

	err = s.readIntegrations(shouldRetrieveTags)
	if err != nil {
		return fmt.Errorf("reading integrations: %w", err)
	}

	err = s.readWarehouses(shouldRetrieveTags)
	if err != nil {
		return fmt.Errorf("reading warehouses: %w", err)
	}

	inboundShares, inboundSharesMap, err := s.readShares(dbExcludes, shouldRetrieveTags)
	if err != nil {
		return fmt.Errorf("reading shares: %w", err)
	}

	s.inboundSharesMap = inboundSharesMap

	databases, err := s.readDatabases(dbExcludes, inboundSharesMap, shouldRetrieveTags)
	if err != nil {
		return fmt.Errorf("reading databases: %w", err)
	}

	// add inboundShares to the list again to fetch their descendants
	databases = append(databases, inboundShares...)

	wp := workerpool.New(getWorkerPoolSize(configParams))

	var merr error

	for _, database := range databases {
		wp.Submit(func() {
			err2 := s.handleDatabase(database)
			if err2 != nil {
				merr = multierror.Append(merr, err2)
			}
		})
	}

	if config.ConfigMap.GetBoolWithDefault(SfApplications, false) {
		wp.Submit(func() {
			applicationExcludes := set.NewSet[string]()
			applicationExcludes.AddSet(dbExcludes)

			for _, share := range inboundShares {
				applicationExcludes.Add(share.Entity.Name)
			}

			err2 := s.readApplications(applicationExcludes)
			if err2 != nil {
				merr = multierror.Append(merr, err2)
			}
		})
	}

	Logger.Info("All databases submitted for processing")

	wp.StopWait()

	Logger.Info("All databases processed")

	if merr != nil {
		return fmt.Errorf("handling databases: %w", merr)
	}

	return nil
}

func (s *DataSourceSyncer) handleDatabase(database ExtendedDbEntity) error {
	Logger.Info(fmt.Sprintf("Handling database %q", database.Entity.Name))

	err := s.setupDatabasePermissions(database.Entity)

	if err != nil {
		return err
	}

	doTypePrefix := ""
	if s.inboundSharesMap.Contains(database.Entity.Name) {
		doTypePrefix = SharedPrefix
	}

	err = s.readSchemasInDatabase(database.Entity.Name, doTypePrefix, database.LinkedTags)
	if err != nil {
		return err
	}

	if doTypePrefix == "" {
		err = s.readFunctionsInDatabase(database.Entity.Name, database.LinkedTags)
		if err != nil {
			return err
		}

		err = s.readProceduresInDatabase(database.Entity.Name, database.LinkedTags)
		if err != nil {
			return err
		}
	}

	err = s.readTablesInDatabase(database.Entity.Name, doTypePrefix, s.repo.GetTablesInDatabase, database.LinkedTags)
	if err != nil {
		return err
	}

	if !s.skipColumns {
		err = s.readColumnsInDatabase(database.Entity.Name, doTypePrefix, database.LinkedTags)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *DataSourceSyncer) readColumnsInDatabase(dbName string, doTypePrefix string, tagMap map[string][]*tag.Tag) error {
	typeName := doTypePrefix + ds.Column

	return s.repo.GetColumnsInDatabase(dbName, func(entity interface{}) error {
		column := entity.(*ColumnEntity)
		schemaName := column.Schema
		schemaFullName := column.Database + "." + schemaName
		ff := s.schemaExcludes.Contains(schemaFullName)
		fs := s.schemaExcludes.Contains(schemaName)

		fullName := schemaFullName + "." + column.Table + "." + column.Name

		if ff || fs || !s.shouldHandle(fullName) {
			Logger.Debug(fmt.Sprintf("Skipping data object (type %s) '%s'", typeName, fullName))
			return nil
		}

		comment := ""
		if column.Comment != nil {
			comment = *column.Comment
		}
		do := ds.DataObject{
			ExternalId:       fullName,
			Name:             column.Name,
			FullName:         fullName,
			Type:             typeName,
			Description:      comment,
			ParentExternalId: schemaFullName + "." + column.Table,
			Tags:             tagMap[fullName],
			DataType:         &column.DataType,
		}

		return s.addDataObjects(&do)
	})
}

func (s *DataSourceSyncer) readSchemasInDatabase(databaseName string, doTypePrefix string, tagMap map[string][]*tag.Tag) error {
	typeName := doTypePrefix + ds.Schema

	return s.repo.GetSchemasInDatabase(databaseName, func(entity interface{}) error {
		schema := entity.(*SchemaEntity)

		fullName := schema.Database + "." + schema.Name

		ff := s.schemaExcludes.Contains(fullName)
		fs := s.schemaExcludes.Contains(schema.Name)

		if ff || fs || !s.shouldHandle(fullName) {
			Logger.Debug(fmt.Sprintf("Skipping data object (type %s) '%s'", typeName, fullName))
			return nil
		}

		comment := ""
		if schema.Comment != nil {
			comment = *schema.Comment
		}
		do := ds.DataObject{
			ExternalId:       fullName,
			Name:             schema.Name,
			FullName:         fullName,
			Type:             typeName,
			Description:      comment,
			ParentExternalId: schema.Database,
			Tags:             tagMap[fullName],
		}

		return s.addDataObjects(&do)
	})
}

func (s *DataSourceSyncer) addDataObjects(dataObjects ...*ds.DataObject) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.dataSourceHandler.AddDataObjects(dataObjects...)
}

// convertFunctionArgumentSignature converts the ARGUMENT_SIGNATURE field of a function to remove the argument names
// For example: "(val VARCHAR, type VARCHAR)" -> "(VARCHAR, VARCHAR)"
func convertFunctionArgumentSignature(signature string) string {
	signature = strings.TrimSpace(signature)
	signature, f := strings.CutPrefix(signature, "(")
	signature, f2 := strings.CutSuffix(signature, ")")

	if !f || !f2 { // Not the expected format
		return signature
	}

	args := strings.Split(signature, ",")
	for i, arg := range args {
		arg = strings.TrimSpace(arg)
		args[i] = arg[strings.LastIndex(arg, " ")+1:]
	}

	return fmt.Sprintf("(%s)", strings.Join(args, ", "))
}

func (s *DataSourceSyncer) createDataObjectForFunction(doType, database, schema, name, argumentSignature string, comment *string, tagMap map[string][]*tag.Tag) *ds.DataObject {
	parent := database + "." + schema
	fullName := parent + `."` + name + `"`

	argumentSignature = convertFunctionArgumentSignature(argumentSignature)

	ff := s.schemaExcludes.Contains(database + "." + schema)

	if ff || !s.shouldHandle(fullName) {
		Logger.Debug(fmt.Sprintf("Skipping data object (type %s) '%s'", doType, fullName))
		return nil
	}

	description := ""
	if comment != nil {
		description = *comment
	}

	do := ds.DataObject{
		ExternalId:       fullName + argumentSignature, // Adding the signature for full uniqueness
		Name:             name + argumentSignature,
		FullName:         fullName + argumentSignature, // Adding the signature because it is needed to reference it when setting grants
		Type:             doType,
		Description:      description,
		ParentExternalId: parent,
		Tags:             tagMap[fullName],
	}

	return &do
}

func (s *DataSourceSyncer) readFunctionsInDatabase(databaseName string, tagMap map[string][]*tag.Tag) error {
	return s.repo.GetFunctionsInDatabase(databaseName, func(entity interface{}) error {
		function := entity.(*FunctionEntity)

		do := s.createDataObjectForFunction(Function, function.Database, function.Schema, function.Name, function.ArgumentSignature, function.Comment, tagMap)
		if do != nil {
			return s.addDataObjects(do)
		}

		return nil
	})
}

func (s *DataSourceSyncer) readProceduresInDatabase(databaseName string, tagMap map[string][]*tag.Tag) error {
	return s.repo.GetProceduresInDatabase(databaseName, func(entity interface{}) error {
		proc := entity.(*ProcedureEntity)

		do := s.createDataObjectForFunction(Procedure, proc.Database, proc.Schema, proc.Name, proc.ArgumentSignature, proc.Comment, tagMap)
		if do != nil {
			return s.addDataObjects(do)
		}

		return nil
	})
}

func (s *DataSourceSyncer) readTablesInDatabase(databaseName string, typePrefix string, fetcher func(dbName string, schemaName string, entityHandler EntityHandler) error, tagMap map[string][]*tag.Tag) error {
	return fetcher(databaseName, "", func(entity interface{}) error {
		table := entity.(*TableEntity)

		typeName := convertSnowflakeTableTypeToRaito(table)
		if typeName == "" {
			return fmt.Errorf("unknown table type '%s'", table.TableType)
		}

		if typePrefix != "" {
			typeName = typePrefix + typeName
		}

		schemaName := table.Schema
		schemaFullName := table.Database + "." + schemaName
		ff := s.schemaExcludes.Contains(schemaFullName)
		fs := s.schemaExcludes.Contains(schemaName)

		fullName := schemaFullName + "." + table.Name

		if ff || fs || !s.shouldHandle(fullName) {
			Logger.Debug(fmt.Sprintf("Skipping data object (type %s) '%s'", typeName, fullName))
			return nil
		}

		comment := ""
		if table.Comment != nil {
			comment = *table.Comment
		}
		do := ds.DataObject{
			ExternalId:       fullName,
			Name:             table.Name,
			FullName:         fullName,
			Type:             typeName,
			Description:      comment,
			ParentExternalId: table.Database + "." + table.Schema,
			Tags:             tagMap[fullName],
		}

		return s.addDataObjects(&do)
	})
}

func (s *DataSourceSyncer) setupDatabasePermissions(db DbEntity) error {
	// grant the SYNC role USAGE/IMPORTED PRIVILEGES on each database so it can query the INFORMATION_SCHEMA
	if s.SfSyncRole != AccountAdmin {
		err := s.repo.ExecuteGrantOnAccountRole("USAGE", fmt.Sprintf("DATABASE %s", common.FormatQuery("%s", db.Name)), s.SfSyncRole, true)

		if err != nil && strings.Contains(err.Error(), "IMPORTED PRIVILEGES") {
			err2 := s.repo.ExecuteGrantOnAccountRole("IMPORTED PRIVILEGES", fmt.Sprintf("DATABASE %s", common.FormatQuery("%s", db.Name)), s.SfSyncRole, true)

			if err2 != nil {
				return err2
			}

			return nil
		} else if err != nil {
			return err
		}

		permissions := []struct {
			permission string
			on         string
		}{
			{"USAGE", fmt.Sprintf("ALL SCHEMAS IN DATABASE %s", common.FormatQuery("%s", db.Name))},
			{"REFERENCES", fmt.Sprintf("ALL TABLES IN DATABASE %s", common.FormatQuery("%s", db.Name))},
			{"REFERENCES", fmt.Sprintf("FUTURE TABLES IN DATABASE %s", common.FormatQuery("%s", db.Name))},
			{"REFERENCES", fmt.Sprintf("ALL EXTERNAL TABLES IN DATABASE %s", common.FormatQuery("%s", db.Name))},
			{"REFERENCES", fmt.Sprintf("FUTURE EXTERNAL TABLES IN DATABASE %s", common.FormatQuery("%s", db.Name))},
			{"REFERENCES", fmt.Sprintf("ALL VIEWS IN DATABASE %s", common.FormatQuery("%s", db.Name))},
			{"REFERENCES", fmt.Sprintf("FUTURE VIEWS IN DATABASE %s", common.FormatQuery("%s", db.Name))},
			{"REFERENCES", fmt.Sprintf("ALL MATERIALIZED VIEWS IN DATABASE %s", common.FormatQuery("%s", db.Name))},
			{"REFERENCES", fmt.Sprintf("FUTURE MATERIALIZED VIEWS IN DATABASE %s", common.FormatQuery("%s", db.Name))},
		}

		for _, perm := range permissions {
			err2 := s.repo.ExecuteGrantOnAccountRole(perm.permission, perm.on, s.SfSyncRole, true)
			if err2 != nil {
				return fmt.Errorf("granting %s on %s: %w", perm.permission, perm.on, err2)
			}
		}
	}

	return nil
}

func (s *DataSourceSyncer) readDatabases(excludes set.Set[string], shares map[string]struct{}, shouldRetrieveTags bool) ([]ExtendedDbEntity, error) {
	databases, err := s.repo.GetDatabases()
	if err != nil {
		return nil, err
	}

	enrichedDatabases, err := s.addTopLevelEntitiesToImporter(databases, ds.Database, shouldRetrieveTags,
		s.repo.GetTagsLinkedToDatabaseName,
		func(name string) string { return name },
		func(name, fullName string) bool {
			_, shared := shares[fullName]
			return !excludes.Contains(fullName) && !shared && s.shouldGoInto(fullName)
		})
	if err != nil {
		return nil, err
	}

	return enrichedDatabases, nil
}

func (s *DataSourceSyncer) readShares(excludes set.Set[string], shouldRetrieveTags bool) ([]ExtendedDbEntity, set.Set[string], error) {
	// main reason is that for export they can only have "IMPORTED PRIVILEGES" granted on the shared db level and nothing else.
	// for now we can just exclude them but they need to be treated later on
	inboundShares, err := s.repo.GetInboundShares()
	if err != nil {
		return nil, nil, err
	}

	enrichedInboundShares, err := s.addTopLevelEntitiesToImporter(inboundShares, "shared-database", shouldRetrieveTags,
		s.repo.GetTagsLinkedToDatabaseName,
		func(name string) string { return name },
		func(name, fullName string) bool {
			return !excludes.Contains(fullName) && s.shouldGoInto(fullName)
		})
	if err != nil {
		return nil, nil, err
	}

	inboundSharesMap := set.NewSet[string]()

	// exclude inboundShares from database import as we treat them separately
	for _, share := range enrichedInboundShares {
		inboundSharesMap.Add(share.Entity.Name)
	}

	return enrichedInboundShares, inboundSharesMap, nil
}

func (s *DataSourceSyncer) readApplications(excludes set.Set[string]) error {
	applications, err := s.repo.GetApplications()
	if err != nil {
		return fmt.Errorf("get applications: %w", err)
	}

	applicationEntities := make([]DbEntity, len(applications))

	for i, app := range applications {
		applicationEntities[i] = DbEntity{
			Name:         app.Name,
			Kind:         ptr.String("APPLICATION"),
			OwnerAccount: app.Owner,
		}
	}

	_, err = s.addTopLevelEntitiesToImporter(applicationEntities, Application, false,
		func(name string) (map[string][]*tag.Tag, error) { return nil, nil },
		func(name string) string { return name },
		func(name, fullName string) bool {
			return !excludes.Contains(fullName)
		})

	if err != nil {
		return fmt.Errorf("add to importert: %w", err)
	}

	return nil
}

func (s *DataSourceSyncer) readWarehouses(shouldRetrieveTags bool) error {
	dbWarehouses, err := s.repo.GetWarehouses()
	if err != nil {
		return err
	}

	allWarehouseTags := make(map[string][]*tag.Tag, 0)

	if shouldRetrieveTags {
		allWarehouseTags, err = s.repo.GetTagsByDomain("WAREHOUSE")

		if err != nil {
			return err
		}
	}

	_, err = s.addTopLevelEntitiesToImporter(dbWarehouses, "warehouse", shouldRetrieveTags,
		func(name string) (map[string][]*tag.Tag, error) {
			return allWarehouseTags, nil
		},
		func(name string) string { return name },
		func(name, fullName string) bool { return s.shouldGoInto(fullName) })
	if err != nil {
		return err
	}

	return nil
}

func (s *DataSourceSyncer) readIntegrations(shouldRetrieveTags bool) error {
	integrations, err := s.repo.GetIntegrations()
	if err != nil {
		return err
	}

	integrationTags := make(map[string][]*tag.Tag, 0)

	if shouldRetrieveTags {
		integrationTags, err = s.repo.GetTagsByDomain("INTEGRATION")

		if err != nil {
			return err
		}
	}

	_, err = s.addTopLevelEntitiesToImporter(integrations, Integration, shouldRetrieveTags,
		func(name string) (map[string][]*tag.Tag, error) {
			return integrationTags, nil
		},
		func(name string) string { return name },
		func(name, fullName string) bool { return s.shouldGoInto(fullName) })
	if err != nil {
		return err
	}

	return nil
}

func (s *DataSourceSyncer) addTopLevelEntitiesToImporter(entities []DbEntity, doType string, shouldRetrieveTags bool, tagRetrieval func(name string) (map[string][]*tag.Tag, error), externalIdGenerator func(name string) string, filter func(name, fullName string) bool) ([]ExtendedDbEntity, error) {
	dbEntities := make([]ExtendedDbEntity, 0, len(entities))

	for _, db := range entities {
		if db.Name == "" {
			continue
		}

		extendedEntity := ExtendedDbEntity{
			Entity: db,
		}

		fullName := externalIdGenerator(extendedEntity.Entity.Name)
		if filter(extendedEntity.Entity.Name, fullName) {
			if shouldRetrieveTags {
				tagMap, err := tagRetrieval(extendedEntity.Entity.Name)

				if err != nil {
					return nil, err
				}

				extendedEntity.LinkedTags = tagMap
			}

			// Potentially, we don't have to handle this object itself but only one of its descendants
			if s.shouldHandle(fullName) {
				comment := ""
				if extendedEntity.Entity.Comment != nil {
					comment = *extendedEntity.Entity.Comment
				}

				var doTags []*tag.Tag = nil

				if extendedEntity.LinkedTags != nil && extendedEntity.LinkedTags[extendedEntity.Entity.Name] != nil {
					doTags = extendedEntity.LinkedTags[extendedEntity.Entity.Name]
				}

				do := ds.DataObject{
					ExternalId:              fullName,
					Name:                    extendedEntity.Entity.Name,
					FullName:                fullName,
					Type:                    doType,
					Description:             comment,
					Tags:                    doTags,
					ShareProviderIdentifier: extendedEntity.Entity.OwnerAccount,
					ShareIdentifier:         extendedEntity.Entity.ShareName,
				}

				err := s.addDataObjects(&do)
				if err != nil {
					return nil, err
				}
			}

			dbEntities = append(dbEntities, extendedEntity)
		} else {
			Logger.Debug(fmt.Sprintf("Skipping data object (type %s) '%s'", doType, fullName))
		}
	}

	return dbEntities, nil
}
