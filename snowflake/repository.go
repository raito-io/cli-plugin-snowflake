package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/blockloop/scan"
	"github.com/hashicorp/go-multierror"
	sf "github.com/snowflakedb/gosnowflake"

	"github.com/raito-io/cli-plugin-snowflake/common"
)

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
	conn      *sql.DB
	queryTime time.Duration
}

func NewSnowflakeRepository(params map[string]interface{}, role string) (*SnowflakeRepository, error) {
	conn, err := ConnectToSnowflake(params, role)
	if err != nil {
		return nil, err
	}

	return &SnowflakeRepository{
		conn: conn,
	}, nil
}

func (repo *SnowflakeRepository) Close() error {
	return repo.conn.Close()
}

func (repo *SnowflakeRepository) TotalQueryTime() time.Duration {
	return repo.queryTime
}

func (repo *SnowflakeRepository) BatchingInformation(startDate *time.Time, historyTable string) (*string, *string, int, error) {
	filterClause := fmt.Sprintf("WHERE start_time > '%s'", startDate.Format(time.RFC3339))
	fetchBatchingInfoQuery := fmt.Sprintf("SELECT min(START_TIME) as minTime, max(START_TIME) as maxTime, COUNT(START_TIME) as numRows FROM %s %s", historyTable, filterClause)

	batchingInfoResult, _, err := repo.query(fetchBatchingInfoQuery)
	if err != nil {
		return nil, nil, 0, err
	}

	var minTime *string
	var maxTime *string
	numRows := 0

	for batchingInfoResult.Next() {
		err := batchingInfoResult.Scan(&minTime, &maxTime, &numRows)
		if err != nil {
			return nil, nil, 0, err
		}

		if numRows == 0 || minTime == nil || maxTime == nil {
			errorMessage := fmt.Sprintf("no usage information available with query: %s => result: numRows: %d, minTime: %v, maxtime: %v",
				fetchBatchingInfoQuery, numRows, minTime, maxTime)
			return nil, nil, 0, fmt.Errorf("%s", errorMessage)
		}
	}

	return minTime, maxTime, numRows, nil
}

func (repo *SnowflakeRepository) DataUsage(columns []string, limit int, offset int, historyTable string, minTime, maxTime *string, accessHistoryAvailable bool) ([]QueryDbEntities, error) {
	filterClause := fmt.Sprintf("WHERE START_TIME >= '%s' and START_TIME <= '%s'", *minTime, *maxTime)
	paginationClause := fmt.Sprintf("LIMIT %d OFFSET %d", limit, offset)

	var query string

	if accessHistoryAvailable {
		logger.Info("Using access history table in combination with history table")
		query = fmt.Sprintf(`SELECT %s, QID, DIRECT_OBJECTS_ACCESSED, BASE_OBJECTS_ACCESSED, OBJECTS_MODIFIED FROM (SELECT %s FROM %s %s) as QUERIES LEFT JOIN (SELECT QUERY_ID as QID, DIRECT_OBJECTS_ACCESSED, BASE_OBJECTS_ACCESSED, OBJECTS_MODIFIED FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY) as ACCESS on QUERIES.QUERY_ID = ACCESS.QID ORDER BY START_TIME, QUERIES.QUERY_ID DESC %s`,
			strings.Join(columns, ", "), strings.Join(columns, ", "), historyTable, filterClause, paginationClause)
	} else {
		query = fmt.Sprintf("SELECT %s FROM %s %s ORDER BY START_TIME, QUERY_ID DESC %s", strings.Join(columns, ", "), historyTable, filterClause, paginationClause)
	}

	logger.Debug(fmt.Sprintf("Retrieving paginated query log from Snowflake with query: %s", query))
	rows, sec, err := repo.query(query)

	if err != nil {
		return nil, err
	}

	var returnedRows []QueryDbEntities
	err = scan.Rows(&returnedRows, rows)

	if err != nil {
		return nil, err
	}

	logger.Info(fmt.Sprintf("Fetched %d rows from Snowflake in %s", len(returnedRows), sec))

	return returnedRows, nil
}

func (repo *SnowflakeRepository) CheckAccessHistoryAvailability(historyTable string) (bool, error) {
	checkAccessHistoryAvailabilityQuery := fmt.Sprintf("SELECT QUERY_ID, DIRECT_OBJECTS_ACCESSED, BASE_OBJECTS_ACCESSED, OBJECTS_MODIFIED FROM %s LIMIT 10", historyTable)

	result, _, err := repo.query(checkAccessHistoryAvailabilityQuery)
	if err != nil {
		return false, err
	}

	numRows := 0
	for result.Next() {
		numRows++
	}

	if numRows > 0 {
		logger.Debug(fmt.Sprintf("Access history query returned %d rows", numRows))
		return true, nil
	}

	return false, nil
}

func (repo *SnowflakeRepository) GetRoles() ([]RoleEntity, error) {
	return repo.GetRolesWithPrefix("")
}

func (repo *SnowflakeRepository) GetRolesWithPrefix(prefix string) ([]RoleEntity, error) {
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

	return roleEntities, nil
}

func (repo *SnowflakeRepository) CreateRole(roleName string) error {
	q := common.FormatQuery(`CREATE ROLE IF NOT EXISTS %s`, roleName)

	_, _, err := repo.query(q)

	return err
}

func (repo *SnowflakeRepository) DropRole(roleName string) error {
	q := common.FormatQuery(`DROP ROLE %s`, roleName)

	_, _, err := repo.query(q)

	return err
}

func (repo *SnowflakeRepository) GetGrantsOfRole(roleName string) ([]GrantOfRole, error) {
	q := common.FormatQuery(`SHOW GRANTS OF ROLE %s`, roleName)

	rows, _, err := repo.query(q)
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

func (repo *SnowflakeRepository) GetGrantsToRole(roleName string) ([]GrantToRole, error) {
	q := common.FormatQuery(`SHOW GRANTS TO ROLE %s`, roleName)

	rows, _, err := repo.query(q)
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

func (repo *SnowflakeRepository) GrantRolesToRole(ctx context.Context, role string, roles ...string) error {
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

func (repo *SnowflakeRepository) RevokeRolesFromRole(ctx context.Context, role string, roles ...string) error {
	statementChan, done := repo.execMultiStatements(ctx)

	for _, otherRole := range roles {
		q := common.FormatQuery(`REVOKE ROLE %s FROM ROLE %s`, role, otherRole)
		statementChan <- q
	}

	close(statementChan)

	return <-done
}

func (repo *SnowflakeRepository) GrantUsersToRole(ctx context.Context, role string, users ...string) error {
	statementChan, done := repo.execMultiStatements(ctx)

	for _, user := range users {
		q := common.FormatQuery(`GRANT ROLE %s TO USER %s`, role, user)
		statementChan <- q
	}

	close(statementChan)

	return <-done
}

func (repo *SnowflakeRepository) RevokeUsersFromRole(ctx context.Context, role string, users ...string) error {
	statementChan, done := repo.execMultiStatements(ctx)

	for _, user := range users {
		q := common.FormatQuery(`REVOKE ROLE %s FROM USER %s`, role, user)
		statementChan <- q
	}

	close(statementChan)

	return <-done
}

func (repo *SnowflakeRepository) ExecuteGrant(perm, on, role string) error {
	// TODO: parse the `on` string correctly, usually it is something like: SCHEMA "db.schema.table"
	q := fmt.Sprintf(`GRANT %s ON %s TO ROLE %s`, perm, on, role)
	logger.Debug("Executing grant query", "query", q)

	_, _, err := repo.query(q)

	if err != nil {
		return fmt.Errorf("error while executing grant query on Snowflake for role %q: %s", role, err.Error())
	}

	return nil
}

func (repo *SnowflakeRepository) ExecuteRevoke(perm, on, role string) error {
	// TODO: parse the `on` string correctly, usually it is something like: SCHEMA "db.schema.table"
	// q := fmt.Sprintf(`REVOKE %s %s`, perm, common.FormatQuery(`ON %s FROM ROLE %s`, on, role))
	q := fmt.Sprintf(`REVOKE %s ON %s FROM ROLE %s`, perm, on, role)
	logger.Debug(fmt.Sprintf("Executing revoke query: %s", q))

	_, _, err := repo.query(q)
	if err != nil {
		return fmt.Errorf("error while executing revoke query on Snowflake for role %q: %s", role, err.Error())
	}

	return nil
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

	return userRows, nil
}

func (repo *SnowflakeRepository) GetPolicies(policy string) ([]policyEntity, error) {
	q := fmt.Sprintf("SHOW %s POLICIES", policy)

	rows, _, err := repo.query(q)
	if err != nil {
		return nil, err
	}

	var policyEntities []policyEntity

	err = scan.Rows(&policyEntities, rows)
	if err != nil {
		return nil, fmt.Errorf("error fetching all masking policies: %s", err.Error())
	}

	logger.Info(fmt.Sprintf("Found %d %s policies", len(policyEntities), policy))

	return policyEntities, nil
}

func (repo *SnowflakeRepository) DescribePolicy(policyType, dbName, schema, policyName string) ([]describePolicyEntity, error) {
	q := common.FormatQuery("DESCRIBE "+policyType+" POLICY %s.%s.%s", dbName, schema, policyName)

	rows, _, err := repo.query(q)
	if err != nil {
		return nil, err
	}

	var desribeMaskingPolicyEntities []describePolicyEntity

	err = scan.Rows(&desribeMaskingPolicyEntities, rows)
	if err != nil {
		return nil, fmt.Errorf("error fetching all %s policies: %s", policyType, err.Error())
	}

	return desribeMaskingPolicyEntities, nil
}

func (repo *SnowflakeRepository) GetPolicyReferences(dbName, schema, policyName string) ([]policyReferenceEntity, error) {
	q := fmt.Sprintf(`select * from table(%s.information_schema.policy_references(policy_name => '%s'))`, dbName, common.FormatQuery(`%s.%s.%s`, dbName, schema, policyName))

	rows, _, err := repo.query(q)
	if err != nil {
		return nil, err
	}

	var policyReferenceEntities []policyReferenceEntity

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

func (repo *SnowflakeRepository) GetDataBases() ([]DbEntity, error) {
	q := "SHOW DATABASES IN ACCOUNT"
	return repo.getDbEntities(q)
}

func (repo *SnowflakeRepository) GetSchemasInDatabase(databaseName string) ([]DbEntity, error) {
	q := getSchemasInDatabaseQuery(databaseName)
	return repo.getDbEntities(q)
}

func (repo *SnowflakeRepository) GetTablesInSchema(sfObject *common.SnowflakeObject) ([]DbEntity, error) {
	q := getTablesInSchemaQuery(sfObject, "TABLES")
	return repo.getDbEntities(q)
}

func (repo *SnowflakeRepository) GetViewsInSchema(sfObject *common.SnowflakeObject) ([]DbEntity, error) {
	q := getTablesInSchemaQuery(sfObject, "VIEWS")
	return repo.getDbEntities(q)
}

func (repo *SnowflakeRepository) GetColumnsInTable(sfObject *common.SnowflakeObject) ([]DbEntity, error) {
	q := getColumnsInTableQuery(sfObject)
	_, err := repo.getDbEntities(q)

	if err != nil {
		return nil, err
	}

	q = "select \"column_name\" as \"name\" from table(result_scan(LAST_QUERY_ID()))"

	return repo.getDbEntities(q)
}

func (repo *SnowflakeRepository) CommentIfExists(comment, objectType, objectName string) error {
	q := fmt.Sprintf(`COMMENT IF EXISTS ON %s %s IS '%s'`, objectType, objectName, comment)

	_, _, err := repo.query(q)

	return err
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

func (repo *SnowflakeRepository) query(query string) (*sql.Rows, time.Duration, error) {
	logger.Debug(fmt.Sprintf("Sending query: %s", query))
	startQuery := time.Now()
	result, err := QuerySnowflake(repo.conn, query)
	sec := time.Since(startQuery).Round(time.Millisecond)
	repo.queryTime += sec

	return result, sec, err
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
	repo.queryTime += sec

	return sec, err
}

func getSchemasInDatabaseQuery(dbName string) string {
	//nolint // %q does not yield expected results
	return fmt.Sprintf(`SHOW SCHEMAS IN DATABASE "%s"`, dbName)
}

func getTablesInSchemaQuery(sfObject *common.SnowflakeObject, tableLevelObject string) string {
	return fmt.Sprintf(`SHOW %s IN SCHEMA %s`, tableLevelObject, sfObject.GetFullName(true))
}

func getColumnsInTableQuery(sfObject *common.SnowflakeObject) string {
	return fmt.Sprintf(`SHOW COLUMNS IN TABLE %s`, sfObject.GetFullName(true))
}
