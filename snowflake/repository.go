package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/smithy-go/ptr"
	"github.com/blockloop/scan"
	"github.com/gammazero/workerpool"
	"github.com/hashicorp/go-multierror"
	"github.com/raito-io/cli/base/tag"
	"github.com/raito-io/golang-set/set"
	sf "github.com/snowflakedb/gosnowflake"

	"github.com/raito-io/cli-plugin-snowflake/common"
	"github.com/raito-io/cli-plugin-snowflake/common/stream"
)

type EntityHandler func(entity interface{}) error
type EntityCreator func() interface{}

func (nullString *NullString) Scan(value interface{}) error {
	var ns sql.NullString
	if err := ns.Scan(value); err != nil {
		return err
	}
	// if nil the make Valid false
	if reflect.TypeOf(value) == nil {
		*nullString = NullString{ns.String, false}
	} else {
		*nullString = NullString{ns.String, true}
	}

	return nil
}

type SnowflakeRepository struct {
	conn           *sql.DB
	queryTime      time.Duration
	role           string
	usageBatchSize int
	workerPoolSize int
	queryTimeLock  sync.Mutex

	maskFactory *MaskFactory
}

func NewSnowflakeRepository(params map[string]string, role string) (*SnowflakeRepository, error) {
	if v, f := params[SfDriverDebug]; f && strings.EqualFold(v, "true") {
		err := sf.GetLogger().SetLogLevel("debug")

		if err != nil {
			logger.Error("Error while setting snowflake sdk to debug level: %s", err.Error())
		}
	}

	workerPoolSize := 10

	if v, f := params[SfWorkerPoolSize]; f {
		poolSize, err := strconv.Atoi(v)
		if err != nil {
			logger.Warn(fmt.Sprintf("Unable to parse parameter %s: %s", SfWorkerPoolSize, err.Error()))
		} else if poolSize > 0 {
			workerPoolSize = poolSize
		}
	}

	conn, role, err := ConnectToSnowflake(params, role)
	if err != nil {
		return nil, err
	}

	usageBatchSize := 100000 // Default batch size 100.000
	if v, f := params[SfUsageBatchSize]; f && v != "" {
		usageBatchSize, err = strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("parsing %q parameter: %w", SfUsageBatchSize, err)
		}

		if usageBatchSize != 0 && (usageBatchSize < 10000 || usageBatchSize > 1000000) {
			return nil, fmt.Errorf("invalid value %d for %q parameter (If set, it must be between 10.000 and 1.000.000, or 0 to disable batching)", usageBatchSize, SfUsageBatchSize)
		}
	}

	return &SnowflakeRepository{
		conn:           conn,
		role:           role,
		usageBatchSize: usageBatchSize,
		workerPoolSize: workerPoolSize,

		maskFactory: NewMaskFactory(),
	}, nil
}

func (repo *SnowflakeRepository) Close() error {
	return repo.conn.Close()
}

func (repo *SnowflakeRepository) TotalQueryTime() time.Duration {
	return repo.queryTime
}

func (repo *SnowflakeRepository) isProtectedRoleName(rn string) bool {
	// if sync role is not account admin, we protect this role both on import & export
	return !strings.EqualFold(repo.role, AccountAdminRole) && strings.EqualFold(repo.role, rn)
}

func (repo *SnowflakeRepository) GetDataUsage(ctx context.Context, minTime time.Time, maxTime *time.Time) <-chan stream.MaybeError[UsageQueryResult] {
	outputChannel := make(chan stream.MaybeError[UsageQueryResult], 10000)

	go func() {
		defer close(outputChannel)

		defer func() {
			if r := recover(); r != nil {
				logger.Error(fmt.Sprintf("Panic data usage processing: %v\n\n%s", r, string(debug.Stack())))

				select {
				case <-ctx.Done():
					return
				case outputChannel <- stream.NewMaybeErrorError[UsageQueryResult](fmt.Errorf("panic during data usage processing: %v", r)):
					return
				}
			}
		}()

		queryGen := func(startTime time.Time) (string, []any) {
			strBuilder := strings.Builder{}
			args := make([]any, 0, 3)

			// First query only the QUERY_HISTORY (to avoid a join without LIMIT)
			strBuilder.WriteString("WITH history as (\n")
			strBuilder.WriteString(`SELECT QUERY_HISTORY.QUERY_ID as QUERY_ID, QUERY_HISTORY.QUERY_TEXT as QUERY_TEXT, DATABASE_NAME, SCHEMA_NAME, QUERY_TYPE, SESSION_ID, QUERY_HISTORY.USER_NAME as USER_NAME, ROLE_NAME, EXECUTION_STATUS, START_TIME, END_TIME, TOTAL_ELAPSED_TIME, BYTES_SCANNED, BYTES_WRITTEN, BYTES_WRITTEN_TO_RESULT, ROWS_PRODUCED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, ROWS_UNLOADED, CREDITS_USED_CLOUD_SERVICES FROM "SNOWFLAKE"."ACCOUNT_USAGE"."QUERY_HISTORY" WHERE START_TIME > ? `)

			args = append(args, startTime)

			if maxTime != nil {
				strBuilder.WriteString("AND START_TIME <= ? ")

				args = append(args, *maxTime)
			}

			if repo.usageBatchSize > 0 {
				strBuilder.WriteString(" ORDER BY START_TIME asc LIMIT ?")

				args = append(args, repo.usageBatchSize)
			}

			strBuilder.WriteString(")")

			// THEN join with ACCESS_HISTORY
			strBuilder.WriteString(` SELECT QUERY_HISTORY.QUERY_ID as QUERY_ID, QUERY_HISTORY.QUERY_TEXT as QUERY_TEXT, DATABASE_NAME, SCHEMA_NAME, QUERY_TYPE, SESSION_ID, QUERY_HISTORY.USER_NAME as USER_NAME, ROLE_NAME, EXECUTION_STATUS, START_TIME, END_TIME, TOTAL_ELAPSED_TIME, BYTES_SCANNED, BYTES_WRITTEN, BYTES_WRITTEN_TO_RESULT, ROWS_PRODUCED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, ROWS_UNLOADED, CREDITS_USED_CLOUD_SERVICES, DIRECT_OBJECTS_ACCESSED, BASE_OBJECTS_ACCESSED, OBJECTS_MODIFIED, OBJECT_MODIFIED_BY_DDL, PARENT_QUERY_ID, ROOT_QUERY_ID 
										FROM history QUERY_HISTORY LEFT JOIN "SNOWFLAKE"."ACCOUNT_USAGE"."ACCESS_HISTORY" ON QUERY_HISTORY.QUERY_ID = ACCESS_HISTORY.QUERY_ID`)

			return strBuilder.String(), args
		}

		i := 0

		totalDuration := time.Duration(0)

		defer func() {
			logger.Info(fmt.Sprintf("Fetched %d rows from Snowflake in %s", i, totalDuration))
		}()

		if repo.usageBatchSize == 0 {
			logger.Info("Fetching data usage without batching")
		} else {
			logger.Info(fmt.Sprintf("Fetching data usage with batch size %d", repo.usageBatchSize))
		}

		for {
			newMostRecentQueryStartTime, numberOfStatements, duration, nextPage := repo.dataUsageBatch(ctx, outputChannel, minTime, queryGen)

			if repo.usageBatchSize != 0 {
				logger.Debug(fmt.Sprintf("Fetched batch of %d rows from Snowflake in %s", numberOfStatements, duration))
			}

			i += numberOfStatements
			totalDuration += duration

			if newMostRecentQueryStartTime != nil {
				minTime = *newMostRecentQueryStartTime
			}

			if repo.usageBatchSize == 0 || !nextPage {
				break
			}
		}
	}()

	return outputChannel
}

func (repo *SnowflakeRepository) dataUsageBatch(ctx context.Context, outputChannel chan<- stream.MaybeError[UsageQueryResult], startTime time.Time, queryGen func(startTime time.Time) (string, []any)) (*time.Time, int, time.Duration, bool) {
	sendError := func(err error) {
		select {
		case <-ctx.Done():
			return
		case outputChannel <- stream.NewMaybeErrorError[UsageQueryResult](err):
			return
		}
	}

	sendObject := func(obj UsageQueryResult) bool {
		select {
		case <-ctx.Done():
			return false
		case outputChannel <- stream.NewMaybeErrorValue[UsageQueryResult](obj):
			return true
		}
	}

	query, args := queryGen(startTime)

	rows, sec, err := repo.queryContext(ctx, query, args...)
	if err != nil {
		sendError(err)
		return nil, 0, 0, false
	}

	defer rows.Close()

	i := 0
	var newMostRecentQueryStartTime *time.Time

	for rows.Next() {
		var result UsageQueryResult

		err = rows.Scan(&result.ExternalId, &result.Query, &result.DatabaseName, &result.SchemaName, &result.QueryType, &result.SessionID, &result.User, &result.Role, &result.Status, &result.StartTime, &result.EndTime, &result.TotalElapsedTime, &result.BytesScanned, &result.BytesWritten, &result.BytesWrittenToResult, &result.RowsProduced, &result.RowsInserted, &result.RowsUpdated, &result.RowsDeleted, &result.RowsUnloaded, &result.CloudCreditsUsed, &result.DirectObjectsAccessed, &result.BaseObjectsAccessed, &result.ObjectsModified, &result.ObjectsModifiedByDdl, &result.ParentQueryID, &result.RootQueryID)
		if err != nil {
			sendError(fmt.Errorf("error while scanning row: %w", err))

			return newMostRecentQueryStartTime, i, sec, false
		}

		ok := sendObject(result)
		if !ok {
			return newMostRecentQueryStartTime, i, sec, false
		}

		i += 1

		if result.StartTime.Valid {
			if newMostRecentQueryStartTime == nil || result.StartTime.Time.After(*newMostRecentQueryStartTime) {
				newMostRecentQueryStartTime = &result.StartTime.Time
			}
		}
	}

	return newMostRecentQueryStartTime, i, sec, repo.usageBatchSize != 0 && i == repo.usageBatchSize
}

func (repo *SnowflakeRepository) GetAccountRoles() ([]RoleEntity, error) {
	return repo.GetAccountRolesWithPrefix("")
}

func (repo *SnowflakeRepository) GetAccountRolesWithPrefix(prefix string) ([]RoleEntity, error) {
	q := "SHOW ROLES"

	if prefix != "" {
		q += " LIKE '" + prefix + "%'"
	}

	rows, _, err := repo.query(q)
	if err != nil {
		return nil, err
	}

	var roleEntities []RoleEntity

	err = scan.Rows(&roleEntities, rows)
	if err != nil {
		return nil, fmt.Errorf("error fetching all roles: %s", err.Error())
	}

	err = CheckSFLimitExceeded(q, len(roleEntities))
	if err != nil {
		return nil, fmt.Errorf("error while finding existing roles: %s", err.Error())
	}

	// filter out role used to sync snowflake to raito
	for i, roleEntity := range roleEntities {
		if repo.isProtectedRoleName(roleEntity.Name) {
			roleEntities[i] = roleEntities[len(roleEntities)-1]
			return roleEntities[:len(roleEntities)-1], nil
		}
	}

	return roleEntities, nil
}

func (repo *SnowflakeRepository) CreateAccountRole(roleName string) error {
	if repo.isProtectedRoleName(roleName) {
		logger.Warn(fmt.Sprintf("skipping mutation of protected role %s", roleName))
		return nil
	}

	q := common.FormatQuery(`CREATE ROLE IF NOT EXISTS %s`, roleName)

	_, _, err := repo.query(q)

	return err
}

func (repo *SnowflakeRepository) DropAccountRole(roleName string) error {
	if repo.isProtectedRoleName(roleName) {
		logger.Warn(fmt.Sprintf("skipping mutation of protected role %s", roleName))
		return nil
	}

	q := common.FormatQuery(`GRANT OWNERSHIP ON ROLE %s TO ROLE %s`, roleName, repo.role)
	_, _, err := repo.query(q)

	if err != nil {
		return err
	}

	q = common.FormatQuery(`DROP ROLE %s`, roleName)
	_, _, err = repo.query(q)

	return err
}

func (repo *SnowflakeRepository) RenameAccountRole(oldName, newName string) error {
	if repo.isProtectedRoleName(oldName) {
		logger.Warn(fmt.Sprintf("skipping mutation of protected role %s", oldName))
		return nil
	}

	q := common.FormatQuery(`ALTER ROLE IF EXISTS %s RENAME TO %s`, oldName, newName)
	_, _, err := repo.query(q)

	return err
}

func (repo *SnowflakeRepository) GetGrantsOfAccountRole(roleName string) ([]GrantOfRole, error) {
	q := common.FormatQuery(`SHOW GRANTS OF ROLE %s`, roleName)

	return repo.grantsOfRoleMapper(q)
}

func (repo *SnowflakeRepository) GetGrantsToAccountRole(roleName string) ([]GrantToRole, error) {
	q := common.FormatQuery(`SHOW GRANTS TO ROLE %s`, roleName)

	return repo.grantsToRoleMapper(q)
}

func (repo *SnowflakeRepository) GrantAccountRolesToAccountRole(ctx context.Context, role string, roles ...string) error {
	statementChan, done := repo.execMultiStatements(ctx)

	for _, otherRole := range roles {
		q := common.FormatQuery(`CREATE ROLE IF NOT EXISTS %s`, otherRole)
		statementChan <- q

		q = common.FormatQuery(`GRANT ROLE %s TO ROLE %s`, role, otherRole)
		statementChan <- q
	}

	close(statementChan)

	return <-done
}

func (repo *SnowflakeRepository) RevokeAccountRolesFromAccountRole(ctx context.Context, accountRole string, accountRoles ...string) error {
	if repo.isProtectedRoleName(accountRole) {
		logger.Warn(fmt.Sprintf("skipping mutation of protected role %s", accountRole))
		return nil
	}

	statementChan, done := repo.execMultiStatements(ctx)

	for _, otherRole := range accountRoles {
		q := common.FormatQuery(`REVOKE ROLE %s FROM ROLE %s`, accountRole, otherRole)
		statementChan <- q
	}

	close(statementChan)

	return <-done
}

func (repo *SnowflakeRepository) GrantUsersToAccountRole(ctx context.Context, role string, users ...string) error {
	statementChan, done := repo.execMultiStatements(ctx)

	for _, user := range users {
		q := common.FormatQuery(`GRANT ROLE %s TO USER %s`, role, user)
		statementChan <- q
	}

	close(statementChan)

	return <-done
}

func (repo *SnowflakeRepository) RevokeUsersFromAccountRole(ctx context.Context, role string, users ...string) error {
	if repo.isProtectedRoleName(role) {
		logger.Warn(fmt.Sprintf("skipping mutation of protected role %s", role))
		return nil
	}

	statementChan, done := repo.execMultiStatements(ctx)

	for _, user := range users {
		q := common.FormatQuery(`REVOKE ROLE %s FROM USER %s`, role, user)
		statementChan <- q
	}

	close(statementChan)

	return <-done
}

func (repo *SnowflakeRepository) ExecuteGrantOnAccountRole(perm, on, accountRole string, isSystemGrant bool) error {
	if repo.isProtectedRoleName(accountRole) && !isSystemGrant {
		logger.Warn(fmt.Sprintf("skipping mutation of protected role %s", accountRole))
		return nil
	}

	// TODO: parse the `on` string correctly, usually it is something like: SCHEMA "db.schema.table"
	q := fmt.Sprintf(`GRANT %s ON %s TO ROLE %s`, perm, on, common.FormatQuery("%s", accountRole))
	logger.Debug("Executing grant query", "query", q)

	_, _, err := repo.query(q)

	if err != nil {
		return fmt.Errorf("error while executing grant query on Snowflake for role %q: %s", accountRole, err.Error())
	}

	return nil
}

func (repo *SnowflakeRepository) ExecuteRevokeOnAccountRole(perm, on, accountRole string, isSystemRevoke bool) error {
	if repo.isProtectedRoleName(accountRole) && !isSystemRevoke {
		logger.Warn(fmt.Sprintf("skipping mutation of protected role %s", accountRole))
		return nil
	}

	// TODO: parse the `on` string correctly, usually it is something like: SCHEMA "db.schema.table"
	// q := fmt.Sprintf(`REVOKE %s %s`, perm, common.FormatQuery(`ON %s FROM ROLE %s`, on, role))
	q := fmt.Sprintf(`REVOKE %s ON %s FROM ROLE %s`, perm, on, common.FormatQuery("%s", accountRole))
	logger.Debug(fmt.Sprintf("Executing revoke query: %s", q))

	_, _, err := repo.query(q)
	if err != nil {
		return fmt.Errorf("error while executing revoke query on Snowflake for role %q: %s", accountRole, err.Error())
	}

	return nil
}

func (repo *SnowflakeRepository) GetDatabaseRoles(database string) ([]RoleEntity, error) {
	return repo.GetDatabaseRolesWithPrefix(database, "")
}

func (repo *SnowflakeRepository) GetDatabaseRolesWithPrefix(database string, prefix string) ([]RoleEntity, error) {
	var roleEntities []RoleEntity

	q := common.FormatQuery(`SHOW DATABASE ROLES IN DATABASE %s`, database)

	if prefix != "" {
		orgQuery, _, err := repo.query(q)
		if err != nil {
			return nil, err
		}

		orgQuery.Close()
		q = fmt.Sprintf(`SELECT * FROM table(RESULT_SCAN(LAST_QUERY_ID())) WHERE "name" like '%s' ORDER BY "created_on" DESC;`, prefix+"%")
	}

	rows, _, err := repo.query(q)
	if err != nil {
		return nil, err
	}

	err = scan.Rows(&roleEntities, rows)
	if err != nil {
		return nil, fmt.Errorf("error fetching all roles: %s", err.Error())
	}

	err = CheckSFLimitExceeded(q, len(roleEntities))
	if err != nil {
		return nil, fmt.Errorf("error while finding existing roles: %s", err.Error())
	}

	// filter out role used to sync snowflake to raito
	for i, roleEntity := range roleEntities {
		if repo.isProtectedRoleName(roleEntity.Name) {
			roleEntities[i] = roleEntities[len(roleEntities)-1]
			return roleEntities[:len(roleEntities)-1], nil
		}
	}

	return roleEntities, nil
}

func (repo *SnowflakeRepository) CreateDatabaseRole(database string, roleName string) error {
	if repo.isProtectedRoleName(roleName) {
		logger.Warn(fmt.Sprintf("skipping mutation of protected role %s.%s", database, roleName))
		return nil
	}

	q := common.FormatQuery(`CREATE DATABASE ROLE IF NOT EXISTS %s.%s`, database, roleName)

	_, _, err := repo.query(q)

	return err
}

func (repo *SnowflakeRepository) DropDatabaseRole(database string, roleName string) error {
	q := common.FormatQuery(`GRANT OWNERSHIP ON DATABASE ROLE %s.%s TO ROLE %s`, database, roleName, repo.role)
	_, _, err := repo.query(q)

	if err != nil {
		return err
	}

	q = common.FormatQuery(`DROP DATABASE ROLE %s.%s`, database, roleName)
	_, _, err = repo.query(q)

	return err
}
func (repo *SnowflakeRepository) RenameDatabaseRole(database, oldName, newName string) error {
	if repo.isProtectedRoleName(oldName) {
		logger.Warn(fmt.Sprintf("skipping mutation of protected role %q.%q", database, oldName))
		return nil
	}

	q := common.FormatQuery(`ALTER DATABASE ROLE IF EXISTS %s.%s RENAME TO %s.%s`, database, oldName, database, newName)
	_, _, err := repo.query(q)

	return err
}

func (repo *SnowflakeRepository) GetGrantsOfDatabaseRole(database, roleName string) ([]GrantOfRole, error) {
	q := common.FormatQuery(`SHOW GRANTS OF DATABASE ROLE %s.%s`, database, roleName)

	return repo.grantsOfRoleMapper(q)
}

func (repo *SnowflakeRepository) GetGrantsToDatabaseRole(database, roleName string) ([]GrantToRole, error) {
	q := common.FormatQuery(`SHOW GRANTS TO DATABASE ROLE %s.%s`, database, roleName)

	return repo.grantsToRoleMapper(q)
}

func (repo *SnowflakeRepository) GrantAccountRolesToDatabaseRole(ctx context.Context, database string, databaseRole string, accountRoles ...string) error {
	statementChan, done := repo.execMultiStatements(ctx)

	for _, otherAccountRole := range accountRoles {
		q := common.FormatQuery(`CREATE ROLE IF NOT EXISTS %s`, otherAccountRole)
		statementChan <- q

		q = common.FormatQuery(`GRANT DATABASE ROLE %s.%s TO ROLE %s`, database, databaseRole, otherAccountRole)
		statementChan <- q
	}

	close(statementChan)

	return <-done
}

func (repo *SnowflakeRepository) GrantDatabaseRolesToDatabaseRole(ctx context.Context, database string, databaseRole string, databaseRoles ...string) error {
	statementChan, done := repo.execMultiStatements(ctx)

	for _, otherDatabaseRole := range databaseRoles {
		q := common.FormatQuery(`CREATE DATABASE ROLE IF NOT EXISTS %s.%s`, database, otherDatabaseRole)
		statementChan <- q

		q = common.FormatQuery(`GRANT DATABASE ROLE %s.%s TO DATABASE ROLE %s.%s`, database, databaseRole, database, otherDatabaseRole)
		statementChan <- q
	}

	close(statementChan)

	return <-done
}

func (repo *SnowflakeRepository) RevokeAccountRolesFromDatabaseRole(ctx context.Context, database string, databaseRole string, accountRoles ...string) error {
	if repo.isProtectedRoleName(databaseRole) {
		logger.Warn(fmt.Sprintf("skipping mutation of protected role %q.%q", database, databaseRole))
		return nil
	}

	statementChan, done := repo.execMultiStatements(ctx)

	for _, otherRole := range accountRoles {
		q := common.FormatQuery(`REVOKE DATABASE ROLE %s.%s FROM ROLE %s`, database, databaseRole, otherRole)
		statementChan <- q
	}

	close(statementChan)

	return <-done
}

func (repo *SnowflakeRepository) RevokeDatabaseRolesFromDatabaseRole(ctx context.Context, database string, databaseRole string, databaseRoles ...string) error {
	if repo.isProtectedRoleName(databaseRole) {
		logger.Warn(fmt.Sprintf("skipping mutation of protected role %q.%q", database, databaseRole))
		return nil
	}

	statementChan, done := repo.execMultiStatements(ctx)

	for _, otherRole := range databaseRoles {
		q := common.FormatQuery(`REVOKE DATABASE ROLE %s.%s FROM DATABASE ROLE %s.%s`, database, databaseRole, database, otherRole)
		statementChan <- q
	}

	close(statementChan)

	return <-done
}

func (repo *SnowflakeRepository) ExecuteGrantOnDatabaseRole(perm, on, database, databaseRole string) error {
	if repo.isProtectedRoleName(databaseRole) && !strings.EqualFold(perm, "USAGE") && !strings.EqualFold(perm, "IMPORTED PRIVILEGES") && !strings.EqualFold(perm, "REFERENCES") {
		logger.Warn(fmt.Sprintf("skipping mutation of protected role %s.%s", database, databaseRole))
		return nil
	}

	// TODO: parse the `on` string correctly, usually it is something like: SCHEMA "db.schema.table"
	q := fmt.Sprintf(`GRANT %s ON %s TO DATABASE ROLE %s`, perm, on, common.FormatQuery("%s.%s", database, databaseRole))
	logger.Debug("Executing grant query", "query", q)

	_, _, err := repo.query(q)

	if err != nil {
		return fmt.Errorf("error while executing grant query on Snowflake for role %s.%s: %s", database, databaseRole, err.Error())
	}

	return nil
}

func (repo *SnowflakeRepository) ExecuteRevokeOnDatabaseRole(perm, on, database, databaseRole string) error {
	if repo.isProtectedRoleName(databaseRole) && !strings.EqualFold(perm, "USAGE") && !strings.EqualFold(perm, "IMPORTED PRIVILEGES") && !strings.EqualFold(perm, "SELECT") {
		logger.Warn(fmt.Sprintf("skipping mutation of protected role %s.%s", database, databaseRole))
		return nil
	}

	// TODO: parse the `on` string correctly, usually it is something like: SCHEMA "db.schema.table"
	// q := fmt.Sprintf(`REVOKE %s %s`, perm, common.FormatQuery(`ON %s FROM ROLE %s`, on, role))
	q := fmt.Sprintf(`REVOKE %s ON %s FROM DATABASE ROLE %s`, perm, on, common.FormatQuery("%s.%s", database, databaseRole))
	logger.Debug(fmt.Sprintf("Executing revoke query: %s", q))

	_, _, err := repo.query(q)
	if err != nil {
		return fmt.Errorf("error while executing revoke query on Snowflake for role %s.%s: %s", database, databaseRole, err.Error())
	}

	return nil
}

func (repo *SnowflakeRepository) grantsOfRoleMapper(query string) ([]GrantOfRole, error) {
	rows, _, err := repo.query(query)
	if err != nil {
		return nil, err
	}

	grantOfEntities := make([]GrantOfRole, 0)

	err = scan.Rows(&grantOfEntities, rows)
	if err != nil {
		logger.Error(err.Error())

		return nil, fmt.Errorf("error fetching grants of role: %s", err.Error())
	}

	return grantOfEntities, nil
}

func (repo *SnowflakeRepository) grantsToRoleMapper(query string) ([]GrantToRole, error) {
	rows, _, err := repo.query(query)
	if err != nil {
		return nil, err
	}

	grantToEntities := make([]GrantToRole, 0)

	err = scan.Rows(&grantToEntities, rows)
	if err != nil {
		logger.Error(err.Error())

		return nil, fmt.Errorf("error fetching grants of role: %s", err.Error())
	}

	return grantToEntities, nil
}

func (repo *SnowflakeRepository) GetUsers() ([]UserEntity, error) {
	q := "SHOW USERS"

	rows, _, err := repo.query(q)
	if err != nil {
		return nil, err
	}

	var userRows []UserEntity

	err = scan.Rows(&userRows, rows)
	if err != nil {
		return nil, err
	}

	if err = CheckSFLimitExceeded(q, len(userRows)); err != nil {
		return nil, fmt.Errorf("error while fetching users: %s", err.Error())
	}

	wp := workerpool.New(repo.workerPoolSize)

	for i := range userRows {
		userRow := userRows[i]
		if userRow.Type != nil && *userRow.Type != "" {
			continue
		}

		wp.Submit(func() {
			describeRows, _, err := repo.query(fmt.Sprintf(`DESCRIBE USER "%s"`, userRow.Name)) //nolint:gocritic
			if err != nil {
				logger.Warn(fmt.Sprintf("Unable to fetch user details for %q: %s", userRow.Name, err.Error()))
				return
			}

			var userDetails []UserDetails
			err = scan.Rows(&userDetails, describeRows)

			if err != nil {
				logger.Warn(fmt.Sprintf("Unable to parse user details for %q: %s", userRow.Name, err.Error()))
				return
			}

			for _, detail := range userDetails {
				if strings.EqualFold(detail.Property, "TYPE") {
					val := detail.Value
					userRow.Type = &val
					userRows[i] = userRow
				}
			}
		})
	}

	wp.StopWait()

	return userRows, nil
}

type UserDetails struct {
	Property string `db:"property"`
	Value    string `db:"value"`
}

func (repo *SnowflakeRepository) GetPolicies(policy string) ([]PolicyEntity, error) {
	q := fmt.Sprintf("SHOW %s POLICIES", policy)

	rows, _, err := repo.query(q)
	if err != nil {
		return nil, err
	}

	var policyEntities []PolicyEntity

	err = scan.Rows(&policyEntities, rows)
	if err != nil {
		return nil, fmt.Errorf("error fetching all masking policies: %s", err.Error())
	}

	logger.Info(fmt.Sprintf("Found %d %s policies", len(policyEntities), policy))

	return policyEntities, nil
}

func (repo *SnowflakeRepository) DescribePolicy(policyType, dbName, schema, policyName string) ([]DescribePolicyEntity, error) {
	q := common.FormatQuery("DESCRIBE "+policyType+" POLICY %s.%s.%s", dbName, schema, policyName)

	rows, _, err := repo.query(q)
	if err != nil {
		return nil, err
	}

	var desribeMaskingPolicyEntities []DescribePolicyEntity

	err = scan.Rows(&desribeMaskingPolicyEntities, rows)
	if err != nil {
		return nil, fmt.Errorf("error fetching all %s policies: %s", policyType, err.Error())
	}

	return desribeMaskingPolicyEntities, nil
}

func (repo *SnowflakeRepository) GetPolicyReferences(dbName, schema, policyName string) ([]PolicyReferenceEntity, error) {
	// to fetch policy references we need to have USAGE on dbName and schema
	if !strings.EqualFold(repo.role, AccountAdminRole) && repo.role != "" {
		err := repo.ExecuteGrantOnAccountRole("USAGE", common.FormatQuery("DATABASE %s", dbName), repo.role, true)

		if err != nil {
			return nil, err
		}

		err = repo.ExecuteGrantOnAccountRole("USAGE", common.FormatQuery("SCHEMA %s.%s", dbName, schema), repo.role, true)

		if err != nil {
			return nil, err
		}
	}

	q := fmt.Sprintf(`select * from table(%s.information_schema.policy_references(policy_name => '%s'))`, dbName, common.FormatQuery(`%s.%s.%s`, dbName, schema, policyName))

	rows, _, err := repo.query(q)
	if err != nil {
		return nil, err
	}

	var policyReferenceEntities []PolicyReferenceEntity

	err = scan.Rows(&policyReferenceEntities, rows)
	if err != nil {
		return nil, err
	}

	return policyReferenceEntities, nil
}

func (repo *SnowflakeRepository) GetSnowFlakeAccountName() (string, error) {
	rows, _, err := repo.query("select current_account()")
	if err != nil {
		return "", err
	}

	var r []string
	err = scan.Rows(&r, rows)

	if err != nil {
		return "", fmt.Errorf("error while querying Snowflake: %s", err.Error())
	}

	if len(r) != 1 {
		return "", fmt.Errorf("error retrieving account information from snowflake")
	}

	return r[0], nil
}

func (repo *SnowflakeRepository) GetTagsByDomain(domain string) (map[string][]*tag.Tag, error) {
	return repo.getTags(ptr.String(domain), nil)
}

func (repo *SnowflakeRepository) GetTagsLinkedToDatabaseName(databaseName string) (map[string][]*tag.Tag, error) {
	return repo.getTags(nil, ptr.String(databaseName))
}

func (repo *SnowflakeRepository) getTags(domain *string, databaseName *string) (map[string][]*tag.Tag, error) {
	tagMap := make(map[string][]*tag.Tag)

	query := []string{}
	additionalWhereItems := ""

	if domain != nil {
		query = append(query, common.FormatQuery(`domain = '%s'`, *domain))
	}

	if databaseName != nil {
		query = append(query, common.FormatQuery("(object_database = '%[1]s' OR (domain = 'DATABASE' AND object_name = '%[1]s'))", *databaseName))
	}

	if len(query) > 0 {
		additionalWhereItems = fmt.Sprintf("AND %s", strings.Join(query, " AND "))
	}

	rows, _, err := repo.query(fmt.Sprintf("select column_name, object_database, object_schema, object_name, domain, tag_name, tag_value from SNOWFLAKE.ACCOUNT_USAGE.tag_references where object_deleted is null %s;", additionalWhereItems))
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		tagEntity := TagEntity{}

		err = scanRow(rows, &tagEntity)
		if err != nil {
			return nil, err
		}

		fullName := tagEntity.GetFullName()
		if fullName != "" {
			tagMap[fullName] = append(tagMap[fullName], tagEntity.CreateTag())
		} else {
			logger.Warn(fmt.Sprintf("skipping tag (%+v) because cannot construct full name", tagEntity))
		}
	}

	return tagMap, nil
}

func (repo *SnowflakeRepository) GetDatabaseRoleTags(databaseName string, roleName string) (map[string][]*tag.Tag, error) {
	tagMap := make(map[string][]*tag.Tag)

	rows, _, err := repo.query(fmt.Sprintf(`
		select column_name, object_database, object_schema, object_name, domain, tag_name, tag_value
		FROM TABLE(%[1]s.INFORMATION_SCHEMA.TAG_REFERENCES('%[1]s.%[2]s','DATABASE ROLE'));`, databaseName, roleName))
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		tagEntity := TagEntity{}

		err = scanRow(rows, &tagEntity)
		if err != nil {
			return nil, err
		}

		fullName := tagEntity.GetFullName()
		if fullName != "" {
			tagMap[fullName] = append(tagMap[fullName], tagEntity.CreateTag())
		} else {
			logger.Warn(fmt.Sprintf("skipping tag (%+v) because cannot construct full name", tagEntity))
		}
	}

	return tagMap, nil
}

func (repo *SnowflakeRepository) GetWarehouses() ([]DbEntity, error) {
	q := "SHOW WAREHOUSES"
	return repo.getDbEntities(q)
}

func (repo *SnowflakeRepository) GetShares() ([]DbEntity, error) {
	q := "SHOW SHARES"
	_, err := repo.getDbEntities(q)

	if err != nil {
		return nil, err
	}

	q = "select \"database_name\" as \"name\" from table(result_scan(LAST_QUERY_ID())) WHERE \"kind\" = 'INBOUND'"

	return repo.getDbEntities(q)
}

func (repo *SnowflakeRepository) GetDatabases() ([]DbEntity, error) {
	q := "SHOW DATABASES IN ACCOUNT"

	dbs, err := repo.getDbEntities(q)
	if err != nil {
		return nil, fmt.Errorf("fetching databases: %w", err)
	}

	ret := make([]DbEntity, 0, len(dbs))

	for _, db := range dbs {
		if db.Kind != nil && strings.EqualFold(*db.Kind, "STANDARD") {
			ret = append(ret, db)
		}
	}

	return ret, nil
}

func (repo *SnowflakeRepository) GetSchemasInDatabase(databaseName string, handleEntity EntityHandler) error {
	q := getSchemasInDatabaseQuery(databaseName)

	return handleDbEntities(repo, q, func() interface{} {
		return &SchemaEntity{}
	}, handleEntity)
}

func (repo *SnowflakeRepository) GetTablesInDatabase(databaseName string, schemaName string, handleEntity EntityHandler) error {
	q := getTablesInDatabaseQuery(databaseName, schemaName)

	return handleDbEntities(repo, q, func() interface{} {
		return &TableEntity{}
	}, handleEntity)
}

func (repo *SnowflakeRepository) GetColumnsInDatabase(databaseName string, handleEntity EntityHandler) error {
	q := getColumnsInDatabaseQuery(databaseName)

	return handleDbEntities(repo, q, func() interface{} {
		return &ColumnEntity{}
	}, handleEntity)
}

func (repo *SnowflakeRepository) CommentAccountRoleIfExists(comment, objectName string) error {
	q := fmt.Sprintf(`COMMENT IF EXISTS ON ROLE %s IS '%s'`, common.FormatQuery("%s", objectName), strings.Replace(comment, "'", "", -1))
	_, _, err := repo.query(q)

	if err != nil {
		logger.Warn(fmt.Sprintf("unable to update comment on role %s, possibly because not owning it. Ignoring: %s ", objectName, err.Error()))
	}

	return nil
}
func (repo *SnowflakeRepository) CommentDatabaseRoleIfExists(comment, database, roleName string) error {
	q := fmt.Sprintf(`COMMENT IF EXISTS ON DATABASE ROLE %s IS '%s'`, common.FormatQuery("%s.%s", database, roleName), strings.Replace(comment, "'", "", -1))
	_, _, err := repo.query(q)

	if err != nil {
		logger.Warn(fmt.Sprintf("unable to update comment on database role %s.%s, possibly because not owning it. Ignoring: %s ", database, roleName, err.Error()))
	}

	return nil
}

func (repo *SnowflakeRepository) CreateMaskPolicy(databaseName string, schema string, maskName string, columnsFullName []string, maskType *string, beneficiaries *MaskingBeneficiaries) (err error) {
	// Ensure we have permission to create masks
	if repo.role != AccountAdminRole {
		err = repo.ExecuteGrantOnAccountRole("CREATE MASKING POLICY", common.FormatQuery("SCHEMA %s.%s", databaseName, schema), repo.role, true)
		if err != nil {
			return err
		}
	}

	dataObjectTypeMap := map[string][]string{}
	columnTypes := set.Set[string]{}

	err = repo.getColumnInformation(databaseName, columnsFullName, func(columnName string, dataType string) error {
		dataObjectTypeMap[dataType] = append(dataObjectTypeMap[dataType], columnName)
		columnTypes.Add(dataType)

		return nil
	})
	if err != nil {
		return err
	}

	tx, err := repo.conn.Begin()
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			tx.Rollback() //nolint
		}
		tx.Commit() //nolint
	}()

	maskingForDataObjects := map[string][]string{}

	// For each column type create a masking policy
	for columnType := range columnTypes {
		maskingName, maskingPolicy, err2 := repo.maskFactory.CreateMask(fmt.Sprintf("%s.%s.%s", databaseName, schema, maskName), columnType, maskType, beneficiaries)
		if err2 != nil {
			return err2
		}

		logger.Debug(fmt.Sprintf("Execute query to create mask %s: '%s'", maskingName, maskingPolicy))

		_, err = tx.Exec(string(maskingPolicy))
		if err != nil {
			return fmt.Errorf("creation of mask %s: %w", maskingName, err)
		}

		_, err = tx.Exec(fmt.Sprintf("GRANT OWNERSHIP ON MASKING POLICY %s TO ROLE %s", maskingName, repo.role))
		if err != nil {
			return err
		}

		maskingForDataObjects[maskingName] = dataObjectTypeMap[columnType]
	}

	// Assign all columns to the correct masking policy
	for maskingName, columns := range maskingForDataObjects {
		for _, column := range columns {
			fullnameSplit := strings.Split(column, ".")

			q := fmt.Sprintf("ALTER TABLE %s.%s.%s ALTER COLUMN %s SET MASKING POLICY %s FORCE", fullnameSplit[0], fullnameSplit[1], fullnameSplit[2], fullnameSplit[3], maskingName)

			logger.Debug(fmt.Sprintf("Execute query to assign mask %s to column %s: '%s'", maskingName, column, q))

			_, err = tx.Exec(q)
			if err != nil {
				return fmt.Errorf("mask %s assignment to column %s: %w", maskingName, column, err)
			}
		}
	}

	return nil
}

func (repo *SnowflakeRepository) GetPoliciesLike(policy string, like string) ([]PolicyEntity, error) {
	q := fmt.Sprintf("SHOW %s POLICIES LIKE '%s';", common.FormatQuery("%s", policy), strings.ToUpper(like))

	var policyEntities []PolicyEntity

	err := handleDbEntities(repo, q, func() interface{} {
		return &PolicyEntity{}
	}, func(entity interface{}) error {
		pEntry := entity.(*PolicyEntity)
		policyEntities = append(policyEntities, *pEntry)

		return nil
	})
	if err != nil {
		return nil, err
	}

	logger.Info(fmt.Sprintf("Found %d %s policies", len(policyEntities), policy))

	return policyEntities, nil
}

func (repo *SnowflakeRepository) DropMaskingPolicy(databaseName string, schema string, maskName string) (err error) {
	policies, err := repo.GetPoliciesLike("MASKING", fmt.Sprintf("%s_%s", maskName, "%"))
	if err != nil {
		return err
	}

	var policyEntries []PolicyReferenceEntity

	for _, policy := range policies {
		entities, err2 := repo.GetPolicyReferences(databaseName, schema, policy.Name)
		if err2 != nil {
			return err2
		}

		policyEntries = append(policyEntries, entities...)
	}

	tx, err := repo.conn.Begin()
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			tx.Rollback() //nolint
		}
		tx.Commit() //nolint
	}()

	for i := range policyEntries {
		_, err = tx.Exec(common.FormatQuery("ALTER TABLE %s.%s.%s ALTER COLUMN %s UNSET MASKING POLICY", databaseName, schema, policyEntries[i].REF_ENTITY_NAME, policyEntries[i].REF_COLUMN_NAME.String))
		if err != nil {
			return err
		}
	}

	for _, policy := range policies {
		if repo.role != AccountAdminRole {
			err = repo.ExecuteGrantOnAccountRole("OWNERSHIP", common.FormatQuery("MASKING POLICY %s.%s.%s", policy.DatabaseName, policy.SchemaName, policy.Name), repo.role, true)
			if err != nil {
				return err
			}
		}

		_, err = tx.Exec(common.FormatQuery("DROP MASKING POLICY %s.%s.%s", policy.DatabaseName, policy.SchemaName, policy.Name))
		if err != nil {
			return err
		}
	}

	return nil
}

func (repo *SnowflakeRepository) UpdateFilter(databaseName string, schema string, tableName string, filterName string, argumentNames []string, expression string) error {
	columnNames := make([]string, 0, len(argumentNames))

	for _, argumentName := range argumentNames {
		columnNames = append(columnNames, fmt.Sprintf("%s.%s.%s.%s", databaseName, schema, tableName, argumentName))
	}

	functionArguments := make([]string, 0, len(argumentNames))

	err := repo.getColumnInformation(databaseName, columnNames, func(columnName string, dataType string) error {
		argumentName := strings.Split(columnName, ".")
		functionArguments = append(functionArguments, fmt.Sprintf("%s %s", argumentName[3], dataType))

		return nil
	})
	if err != nil {
		return err
	}

	if len(functionArguments) != len(argumentNames) {
		return fmt.Errorf("number of function arguments (%d) does not match number of argument names (%d)", len(functionArguments), len(argumentNames))
	}

	existingPolicy, err := repo.getRowFilterForTableIfExists(databaseName, schema, tableName)
	if err != nil {
		return fmt.Errorf("load possible existing row filter: %w", err)
	}

	var dropOldPolicy string
	var deleteOldPolicy *string

	if existingPolicy != nil {
		dropOldPolicy = common.FormatQuery("DROP ROW ACCESS POLICY %s.%s.%s,", databaseName, schema, *existingPolicy)
		deleteOldPolicy = ptr.String(common.FormatQuery("DROP ROW ACCESS POLICY IF EXISTS %s.%s.%s;", databaseName, schema, *existingPolicy))
	}

	if repo.role != AccountAdminRole {
		err = repo.ExecuteGrantOnAccountRole("CREATE ROW ACCESS POLICY", common.FormatQuery("SCHEMA %s.%s", databaseName, schema), repo.role, true)
		if err != nil {
			return err
		}
	}

	q := make([]string, 0, 3)
	q = append(q, fmt.Sprintf(`CREATE ROW ACCESS POLICY %s AS (%s) returns boolean ->
			%s;`, common.FormatQuery("%s.%s.%s", databaseName, schema, filterName), strings.Join(functionArguments, ", "), expression),
		common.FormatQuery("ALTER TABLE %[1]s.%[2]s.%[3]s %[4]s ADD ROW ACCESS POLICY %[1]s.%[2]s.%[5]s on (%[6]s);", databaseName, schema, tableName, dropOldPolicy, filterName, strings.Join(argumentNames, ", ")))

	if deleteOldPolicy != nil {
		q = append(q, *deleteOldPolicy)
	}

	err = repo.execute(q...)
	if err != nil {
		return err
	}

	return nil
}

func (repo *SnowflakeRepository) DropFilter(databaseName string, schema string, tableName string, filterName string) error {
	existingPolicy, err := repo.getRowFilterForTableIfExists(databaseName, schema, tableName)
	if err != nil {
		return fmt.Errorf("load possible existing row filter: %w", err)
	}

	if repo.role != AccountAdminRole {
		err = repo.ExecuteGrantOnAccountRole("OWNERSHIP", common.FormatQuery("ROW ACCESS POLICY %s.%s.%s", databaseName, schema, filterName), repo.role, true)
		if err != nil {
			return err
		}
	}

	if existingPolicy != nil {
		err = repo.execute(common.FormatQuery("ALTER TABLE %[1]s.%[2]s.%[3]s DROP ROW ACCESS POLICY %[1]s.%[2]s.%[4]s;", databaseName, schema, tableName, *existingPolicy))
		if err != nil {
			return err
		}
	}

	err = repo.execute(
		common.FormatQuery(`DROP ROW ACCESS POLICY IF EXISTS %s.%s.%s;`, databaseName, schema, filterName),
	)
	if err != nil {
		return err
	}

	return nil
}

func (repo *SnowflakeRepository) getRowFilterForTableIfExists(databaseName string, schema string, tableName string) (*string, error) {
	_, _, err := repo.query(common.FormatQuery("USE DATABASE %s;", databaseName))
	if err != nil {
		return nil, fmt.Errorf("connect to database %q: %w", databaseName, err)
	}

	q := common.FormatQuery(`select POLICY_NAME from table(%s.information_schema.policy_references(REF_ENTITY_NAME => '%s.%s.%s', REF_ENTITY_DOMAIN => 'table')) WHERE POLICY_KIND = 'ROW_ACCESS_POLICY'`, databaseName, databaseName, schema, tableName)

	rows, _, err := repo.query(q)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}

	if !rows.Next() {
		return nil, nil
	}

	var policyName string

	err = rows.Scan(&policyName)
	if err != nil {
		return nil, fmt.Errorf("error while scanning row: %w", err)
	}

	return &policyName, nil
}

func (repo *SnowflakeRepository) getDbEntities(query string) ([]DbEntity, error) {
	rows, _, err := repo.query(query)
	if err != nil {
		return nil, err
	}

	var dbs []DbEntity
	err = scan.Rows(&dbs, rows)

	if err != nil {
		return nil, err
	}

	err = CheckSFLimitExceeded(query, len(dbs))
	if err != nil {
		return nil, fmt.Errorf("error while fetching db entitities: %s", err.Error())
	}

	return dbs, nil
}

func (repo *SnowflakeRepository) queryContext(ctx context.Context, query string, args ...any) (*sql.Rows, time.Duration, error) {
	logger.Debug(fmt.Sprintf("Sending query: %s", query))
	startQuery := time.Now()
	result, err := repo.conn.QueryContext(ctx, query, args...)
	sec := time.Since(startQuery).Round(time.Millisecond)
	repo.addToQueryTime(sec)

	logger.Debug(fmt.Sprintf("Query took %s", time.Since(startQuery)))

	return result, sec, err
}

func (repo *SnowflakeRepository) query(query string) (*sql.Rows, time.Duration, error) { //nolint:unparam
	logger.Debug(fmt.Sprintf("Sending query: %s", query))
	startQuery := time.Now()
	result, err := QuerySnowflake(repo.conn, query)
	sec := time.Since(startQuery).Round(time.Millisecond)

	repo.addToQueryTime(sec)

	logger.Debug(fmt.Sprintf("Query took %s", time.Since(startQuery)))

	return result, sec, err
}

func (repo *SnowflakeRepository) addToQueryTime(duration time.Duration) {
	repo.queryTimeLock.Lock()
	repo.queryTime += duration
	repo.queryTimeLock.Unlock()
}

func (repo *SnowflakeRepository) execute(query ...string) error {
	logger.Debug(fmt.Sprintf("Sending query execution: %v", query))

	for i := range query {
		if !strings.HasSuffix(query[i], ";") {
			query[i] += ";"
		}
	}

	ctx, err := sf.WithMultiStatement(context.Background(), len(query))
	if err != nil {
		return err
	}

	startQuery := time.Now()
	err = ExecuteSnowflake(ctx, repo.conn, strings.Join(query, "\n"))
	sec := time.Since(startQuery).Round(time.Millisecond)
	repo.addToQueryTime(sec)

	return err
}

func (repo *SnowflakeRepository) execMultiStatements(ctx context.Context) (chan string, chan error) {
	maxStatementsPerTransaction := 200

	statementChannel := make(chan string, maxStatementsPerTransaction)
	done := make(chan error)

	go func() {
		statements := make([]string, 0, maxStatementsPerTransaction)

		var statementError error

		var totalDuration time.Duration
		totalStatements := 0

		for {
			statement, more := <-statementChannel
			if more {
				statements = append(statements, statement)
				if len(statements) == maxStatementsPerTransaction {
					sec, err := repo.execContext(ctx, statements)
					if err != nil {
						statementError = multierror.Append(statementError, err)
					}

					totalDuration += sec
					totalStatements += maxStatementsPerTransaction
					statements = make([]string, 0, maxStatementsPerTransaction)
				}
			} else {
				if len(statements) > 0 {
					sec, err := repo.execContext(ctx, statements)
					if err != nil {
						statementError = multierror.Append(statementError, err)
					}

					totalDuration += sec
					totalStatements += len(statements)
				}
				done <- statementError

				break
			}
		}

		logger.Debug(fmt.Sprintf("executed %d statements in %s", totalStatements, totalDuration))
	}()

	return statementChannel, done
}

func (repo *SnowflakeRepository) execContext(ctx context.Context, statements []string) (time.Duration, error) {
	multiContext, _ := sf.WithMultiStatement(ctx, len(statements))

	query := strings.Join(statements, "; ")
	logger.Debug(fmt.Sprintf("Sending queries: %s", query))

	startQuery := time.Now()
	_, err := repo.conn.ExecContext(multiContext, query)
	sec := time.Since(startQuery).Round(time.Millisecond)
	repo.addToQueryTime(sec)

	if err != nil {
		return sec, fmt.Errorf("error while executing queries: %s: %w", query, err)
	}

	return sec, nil
}

func (repo *SnowflakeRepository) getColumnInformation(databaseName string, columnFullNames []string, fn func(columnName string, dataType string) error) error {
	if len(columnFullNames) == 0 {
		return nil
	}

	columnLiterats := make([]string, 0, len(columnFullNames))
	for _, fullName := range columnFullNames {
		columnLiterats = append(columnLiterats, fmt.Sprintf("'%s'", fullName))
	}

	q := fmt.Sprintf("SELECT * FROM %s.INFORMATION_SCHEMA.COLUMNS WHERE CONCAT_WS('.', TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME) IN (%s)", databaseName, strings.Join(columnLiterats, ", "))

	err := handleDbEntities(repo, q, func() interface{} { return &ColumnEntity{} }, func(entity interface{}) error {
		columnEntity := entity.(*ColumnEntity)
		fullName := strings.Join([]string{columnEntity.Database, columnEntity.Schema, columnEntity.Table, columnEntity.Name}, ".")

		return fn(fullName, columnEntity.DataType)
	})
	if err != nil {
		return err
	}

	return nil
}

func handleDbEntities(repo *SnowflakeRepository, query string, createEntity EntityCreator, handleEntity EntityHandler) error {
	rows, _, err := repo.query(query)
	if err != nil {
		return err
	}

	for rows.Next() {
		entity := createEntity()

		err = scanRow(rows, entity)

		if err != nil {
			return err
		}

		err = handleEntity(entity)
		if err != nil {
			return err
		}
	}

	return nil
}

func getSchemasInDatabaseQuery(dbName string) string {
	return fmt.Sprintf(`SELECT * FROM %s.INFORMATION_SCHEMA.SCHEMATA`, common.FormatQuery("%s", dbName))
}

func getTablesInDatabaseQuery(dbName string, schemaName string) string {
	whereClause := ""
	if schemaName != "" {
		whereClause += fmt.Sprintf(`WHERE TABLE_SCHEMA = '%s'`, schemaName)
	}

	return fmt.Sprintf(`SELECT * FROM %s.INFORMATION_SCHEMA.TABLES %s`, common.FormatQuery("%s", dbName), whereClause)
}

func getColumnsInDatabaseQuery(dbName string) string {
	return fmt.Sprintf(`SELECT * FROM %s.INFORMATION_SCHEMA.COLUMNS`, common.FormatQuery("%s", dbName))
}

func scanRow(rows *sql.Rows, dest interface{}) error {
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer")
	}

	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	fieldPtrs := make([]interface{}, len(columns))

	for i := 0; i < len(columns); i++ {
		fieldName := columns[i]
		field := v.Elem().FieldByNameFunc(func(s string) bool {
			field, _ := v.Elem().Type().FieldByName(s)
			return field.Tag.Get("db") == fieldName
		})

		if field.IsValid() {
			fieldPtrs[i] = field.Addr().Interface()
		} else {
			// If the field is not found in the struct, use a placeholder to ignore the column
			var placeholder interface{}
			fieldPtrs[i] = &placeholder
		}
	}

	return rows.Scan(fieldPtrs...)
}
