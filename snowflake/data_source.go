package snowflake

import (
	"context"
	"fmt"
	"strings"
	"time"

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
	GetSnowFlakeAccountName() (string, error)
	GetWarehouses() ([]DbEntity, error)
	GetShares() ([]DbEntity, error)
	GetDatabases() ([]DbEntity, error)
	GetSchemasInDatabase(databaseName string, handleEntity EntityHandler) error
	GetTablesInDatabase(databaseName string, schemaName string, handleEntity EntityHandler) error
	GetColumnsInDatabase(databaseName string, handleEntity EntityHandler) error
	GetTagsLinkedToDatabaseName(databaseName string) (map[string][]*tag.Tag, error)
	GetTagsByDomain(domain string) (map[string][]*tag.Tag, error)
	ExecuteGrantOnAccountRole(perm, on, role string) error
}

type DataSourceSyncer struct {
	repoProvider func(params map[string]string, role string) (dataSourceRepository, error)
	SfSyncRole   string

	startFrom       string
	excludeChildren []string
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
	configParams := config.ConfigMap

	s.startFrom = config.DataObjectParent
	s.excludeChildren = config.DataObjectExcludes

	repo, err := s.repoProvider(configParams.Parameters, "")
	if err != nil {
		return err
	}

	defer func() {
		logger.Info(fmt.Sprintf("Total snowflake query time:  %s", repo.TotalQueryTime()))
		repo.Close()
	}()

	// for data source level access import & export convenience we retrieve the snowflake account and use it as datasource name
	sfAccount, err := repo.GetSnowFlakeAccountName()
	if err != nil {
		return err
	}

	s.SfSyncRole = configParams.GetStringWithDefault(SfRole, AccountAdmin)

	dataSourceHandler.SetDataSourceName(sfAccount)
	dataSourceHandler.SetDataSourceFullname(sfAccount)

	standard := configParams.GetBoolWithDefault(SfStandardEdition, false)
	skipTags := configParams.GetBoolWithDefault(SfSkipTags, false)
	skipColumns := configParams.GetBoolWithDefault(SfSkipColumns, false)

	excludedDatabases := "SNOWFLAKE"
	if v, ok := configParams.Parameters[SfExcludedDatabases]; ok {
		excludedDatabases = v
	}

	dbExcludes := parseCommaSeparatedList(excludedDatabases)

	excludedSchemaList := "INFORMATION_SCHEMA"
	if v, ok := configParams.Parameters[SfExcludedSchemas]; ok {
		excludedSchemaList += "," + v
	}

	schemaExcludes := parseCommaSeparatedList(excludedSchemaList)
	shouldRetrieveTags := !standard && !skipTags

	err = s.readWarehouses(repo, dataSourceHandler, shouldRetrieveTags)
	if err != nil {
		return err
	}

	shares, sharesMap, err := s.readShares(repo, dbExcludes, dataSourceHandler, shouldRetrieveTags)
	if err != nil {
		return err
	}

	databases, err := s.readDatabases(repo, dbExcludes, dataSourceHandler, sharesMap, shouldRetrieveTags)
	if err != nil {
		return err
	}

	// add shares to the list again to fetch their descendants
	databases = append(databases, shares...)

	for _, database := range databases {
		logger.Info(fmt.Sprintf("Handling database %q", database.Entity.Name))

		err := s.setupDatabasePermissions(repo, database.Entity)

		if err != nil {
			return err
		}

		doTypePrefix := ""
		if _, f := sharesMap[database.Entity.Name]; f {
			doTypePrefix = SharedPrefix
		}

		err = s.readSchemasInDatabase(repo, database.Entity.Name, schemaExcludes, dataSourceHandler, doTypePrefix, database.LinkedTags)
		if err != nil {
			return err
		}

		err = s.readTablesInDatabase(database.Entity.Name, schemaExcludes, dataSourceHandler, doTypePrefix, repo.GetTablesInDatabase, database.LinkedTags)
		if err != nil {
			return err
		}

		if !skipColumns {
			err = s.readColumnsInDatabase(repo, database.Entity.Name, schemaExcludes, dataSourceHandler, doTypePrefix, database.LinkedTags)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *DataSourceSyncer) readColumnsInDatabase(repo dataSourceRepository, dbName string, excludedSchemas set.Set[string], dataSourceHandler wrappers.DataSourceObjectHandler, doTypePrefix string, tagMap map[string][]*tag.Tag) error {
	typeName := doTypePrefix + ds.Column

	return repo.GetColumnsInDatabase(dbName, func(entity interface{}) error {
		column := entity.(*ColumnEntity)
		schemaName := column.Schema
		schemaFullName := column.Database + "." + schemaName
		ff := excludedSchemas.Contains(schemaFullName)
		fs := excludedSchemas.Contains(schemaName)

		fullName := schemaFullName + "." + column.Table + "." + column.Name

		if ff || fs || !s.shouldHandle(fullName) {
			logger.Debug(fmt.Sprintf("Skipping data object (type %s) '%s'", typeName, fullName))
			return nil
		}

		logger.Debug(fmt.Sprintf("Handling data object (type %s) '%s'", typeName, fullName))

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

		return dataSourceHandler.AddDataObjects(&do)
	})
}

func (s *DataSourceSyncer) readSchemasInDatabase(repo dataSourceRepository, databaseName string, excludedSchemas set.Set[string], dataSourceHandler wrappers.DataSourceObjectHandler, doTypePrefix string, tagMap map[string][]*tag.Tag) error {
	typeName := doTypePrefix + ds.Schema

	return repo.GetSchemasInDatabase(databaseName, func(entity interface{}) error {
		schema := entity.(*SchemaEntity)

		fullName := schema.Database + "." + schema.Name

		ff := excludedSchemas.Contains(fullName)
		fs := excludedSchemas.Contains(schema.Name)

		if ff || fs || !s.shouldHandle(fullName) {
			logger.Debug(fmt.Sprintf("Skipping data object (type %s) '%s'", typeName, fullName))
			return nil
		}

		logger.Debug(fmt.Sprintf("Handling data object (type %s) '%s'", typeName, fullName))

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

		return dataSourceHandler.AddDataObjects(&do)
	})
}

func (s *DataSourceSyncer) readTablesInDatabase(databaseName string, excludedSchemas set.Set[string], dataSourceHandler wrappers.DataSourceObjectHandler, typePrefix string, fetcher func(dbName string, schemaName string, entityHandler EntityHandler) error, tagMap map[string][]*tag.Tag) error {
	return fetcher(databaseName, "", func(entity interface{}) error {
		table := entity.(*TableEntity)

		typeName := convertSnowflakeTableTypeToRaito(table.TableType)
		if typeName == "" {
			return fmt.Errorf("unknown table type '%s'", table.TableType)
		}

		if typePrefix != "" {
			typeName = typePrefix + typeName
		}

		schemaName := table.Schema
		schemaFullName := table.Database + "." + schemaName
		ff := excludedSchemas.Contains(schemaFullName)
		fs := excludedSchemas.Contains(schemaName)

		fullName := schemaFullName + "." + table.Name

		if ff || fs || !s.shouldHandle(fullName) {
			logger.Debug(fmt.Sprintf("Skipping data object (type %s) '%s'", typeName, fullName))
			return nil
		}

		logger.Debug(fmt.Sprintf("Handling data object (type %s) '%s'", typeName, fullName))

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

		return dataSourceHandler.AddDataObjects(&do)
	})
}

func (s *DataSourceSyncer) setupDatabasePermissions(repo dataSourceRepository, db DbEntity) error {
	// grant the SYNC role USAGE/IMPORTED PRIVILEGES on each database so it can query the INFORMATION_SCHEMA
	if s.SfSyncRole != AccountAdmin {
		err := repo.ExecuteGrantOnAccountRole("USAGE", fmt.Sprintf("DATABASE %s", common.FormatQuery("%s", db.Name)), s.SfSyncRole)

		if err != nil && strings.Contains(err.Error(), "IMPORTED PRIVILEGES") {
			err2 := repo.ExecuteGrantOnAccountRole("IMPORTED PRIVILEGES", fmt.Sprintf("DATABASE %s", common.FormatQuery("%s", db.Name)), s.SfSyncRole)

			if err2 != nil {
				return err2
			}
		} else if err != nil {
			return err
		} else {
			err2 := repo.ExecuteGrantOnAccountRole("USAGE", fmt.Sprintf("ALL SCHEMAS IN DATABASE %s", common.FormatQuery("%s", db.Name)), s.SfSyncRole)

			if err2 != nil {
				return err2
			}

			err2 = repo.ExecuteGrantOnAccountRole("REFERENCES", fmt.Sprintf("ALL TABLES IN DATABASE %s", common.FormatQuery("%s", db.Name)), s.SfSyncRole)

			if err2 != nil {
				return err2
			}

			err2 = repo.ExecuteGrantOnAccountRole("REFERENCES", fmt.Sprintf("ALL EXTERNAL TABLES IN DATABASE %s", common.FormatQuery("%s", db.Name)), s.SfSyncRole)

			if err2 != nil {
				return err2
			}

			err2 = repo.ExecuteGrantOnAccountRole("REFERENCES", fmt.Sprintf("ALL VIEWS IN DATABASE %s", common.FormatQuery("%s", db.Name)), s.SfSyncRole)

			if err2 != nil {
				return err2
			}

			err2 = repo.ExecuteGrantOnAccountRole("REFERENCES", fmt.Sprintf("ALL MATERIALIZED VIEWS IN DATABASE %s", common.FormatQuery("%s", db.Name)), s.SfSyncRole)

			if err2 != nil {
				return err2
			}
		}
	}

	return nil
}

func (s *DataSourceSyncer) readDatabases(repo dataSourceRepository, excludes set.Set[string], dataSourceHandler wrappers.DataSourceObjectHandler, shares map[string]struct{}, shouldRetrieveTags bool) ([]ExtendedDbEntity, error) {
	databases, err := repo.GetDatabases()
	if err != nil {
		return nil, err
	}

	enrichedDatabases, err := s.addDbEntitiesToImporter(dataSourceHandler, databases, ds.Database, "", shouldRetrieveTags,
		repo.GetTagsLinkedToDatabaseName,
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

func (s *DataSourceSyncer) readShares(repo dataSourceRepository, excludes set.Set[string], dataSourceHandler wrappers.DataSourceObjectHandler, shouldRetrieveTags bool) ([]ExtendedDbEntity, map[string]struct{}, error) {
	// main reason is that for export they can only have "IMPORTED PRIVILEGES" granted on the shared db level and nothing else.
	// for now we can just exclude them but they need to be treated later on
	shares, err := repo.GetShares()
	if err != nil {
		return nil, nil, err
	}

	enrichedShares, err := s.addDbEntitiesToImporter(dataSourceHandler, shares, "shared-database", "", shouldRetrieveTags,
		repo.GetTagsLinkedToDatabaseName,
		func(name string) string { return name },
		func(name, fullName string) bool {
			return !excludes.Contains(fullName) && s.shouldGoInto(fullName)
		})
	if err != nil {
		return nil, nil, err
	}

	sharesMap := make(map[string]struct{}, 0)

	// exclude shares from database import as we treat them separately
	for _, share := range enrichedShares {
		sharesMap[share.Entity.Name] = struct{}{}
	}

	return enrichedShares, sharesMap, nil
}

func (s *DataSourceSyncer) readWarehouses(repo dataSourceRepository, dataSourceHandler wrappers.DataSourceObjectHandler, shouldRetrieveTags bool) error {
	dbWarehouses, err := repo.GetWarehouses()
	if err != nil {
		return err
	}

	allWarehouseTags := make(map[string][]*tag.Tag, 0)

	if shouldRetrieveTags {
		allWarehouseTags, err = repo.GetTagsByDomain("WAREHOUSE")

		if err != nil {
			return err
		}
	}

	_, err = s.addDbEntitiesToImporter(dataSourceHandler, dbWarehouses, "warehouse", "", shouldRetrieveTags,
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

func (s *DataSourceSyncer) addDbEntitiesToImporter(dataObjectHandler wrappers.DataSourceObjectHandler, entities []DbEntity, doType string, parent string, shouldRetrieveTags bool, tagRetrieval func(name string) (map[string][]*tag.Tag, error), externalIdGenerator func(name string) string, filter func(name, fullName string) bool) ([]ExtendedDbEntity, error) {
	dbEntities := make([]ExtendedDbEntity, 0, 20)

	for _, db := range entities {
		if db.Name == "" {
			continue
		}

		extendedEntity := ExtendedDbEntity{
			Entity: db,
		}

		logger.Debug(fmt.Sprintf("Handling data object (type %s) '%s'", doType, extendedEntity.Entity.Name))

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
					ExternalId:       fullName,
					Name:             extendedEntity.Entity.Name,
					FullName:         fullName,
					Type:             doType,
					Description:      comment,
					ParentExternalId: parent,
					Tags:             doTags,
				}

				err := dataObjectHandler.AddDataObjects(&do)
				if err != nil {
					return nil, err
				}
			}

			dbEntities = append(dbEntities, extendedEntity)
		} else {
			logger.Debug(fmt.Sprintf("Skipping data object (type %s) '%s'", doType, fullName))
		}
	}

	return dbEntities, nil
}
