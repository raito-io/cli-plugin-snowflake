package main

import (
	"database/sql"
	"flag"
	"fmt"

	"github.com/blockloop/scan"
	"github.com/raito-io/golang-set/set"
	sf "github.com/snowflakedb/gosnowflake"

	"github.com/raito-io/cli-plugin-snowflake/common"
	"github.com/raito-io/cli-plugin-snowflake/snowflake"
)

var systemRoles = set.Set[string]{"ORGADMIN": {}, "ACCOUNTADMIN": {}, "SECURITYADMIN": {}, "USERADMIN": {}, "SYSADMIN": {}, "PUBLIC": {}, "RAITO_SYNC": {}}

const (
	sfRole = "ACCOUNTADMIN"
)

var (
	sfAccount, sfOrganization, sfUser, sfPassword, sfPrivateKeyFile string
	nonDryRun                                                       bool
)

func dropAllRoles() error {
	account := sfOrganization + "-" + sfAccount

	fmt.Printf("Using account: %s\n", account)

	role := sfRole

	config := sf.Config{
		Account: account,
		User:    sfUser,
		Role:    role,
	}

	if sfPrivateKeyFile != "" {
		key, err := snowflake.LoadPrivateKeyFromFile(sfPrivateKeyFile, "")
		if err != nil {
			return fmt.Errorf("load private key: %w", err)
		}

		config.PrivateKey = key
		config.Authenticator = sf.AuthTypeJwt
	} else {
		config.Password = sfPassword
	}

	dsn, err := sf.DSN(&config)

	if err != nil {
		return fmt.Errorf("snowflake dsn: %w", err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return fmt.Errorf("open snowflake: %w", err)
	}

	defer db.Close()

	err = dropOutboundShares(db, role)
	if err != nil {
		return err
	}

	err = dropAccountRoles(db)
	if err != nil {
		return err
	}

	err = dropDatabaseRoles(db)
	if err != nil {
		return err
	}

	return nil
}

func dropAccountRoles(db *sql.DB) error {
	existingRoles, err := loadRoles(db)
	if err != nil {
		return fmt.Errorf("load roles: %w", err)
	}

	for i, existingRole := range existingRoles {
		if existingRole.Owner == "" || systemRoles.Contains(existingRole.Name) {
			fmt.Printf("ignore role %q %d/%d\n", existingRole.Name, i+1, len(existingRoles))
			continue
		}

		fmt.Printf("dropping role %q %d/%d\n", existingRole.Name, i+1, len(existingRoles))
		err = dropRole(db, existingRole, nil)
		if err != nil {
			return fmt.Errorf("drop role %s: %w", existingRole.Name, err)
		}
	}

	return nil
}

func dropDatabaseRoles(db *sql.DB) error {
	databases, err := loadDatabases(db)
	if err != nil {
		return fmt.Errorf("load databases: %w", err)
	}

	for _, database := range databases {
		if database.Owner == "" {
			fmt.Printf("ignore database %q\n", database.Name)
			continue
		}

		databaseRoles, err2 := loadDatabaseRoles(db, database.Name)
		if err2 != nil {
			return fmt.Errorf("load database roles: %w", err2)
		}

		for i, databaseRole := range databaseRoles {
			fmt.Printf("dropping role %q in databasse %q %d/%d\n", databaseRole.Name, database.Name, i+1, len(databaseRoles))
			err = dropRole(db, databaseRole, &database.Name)
			if err != nil {
				return fmt.Errorf("drop role %s in database %s: %w", databaseRole.Name, database.Name, err)
			}
		}
	}

	return nil
}

func dropOutboundShares(db *sql.DB, currentRole string) error {
	rows, err := db.Query("SHOW SHARES")
	if err != nil {
		return fmt.Errorf("query snowflake shares: %w", err)
	}

	rows.Close()

	rows, err = db.Query("select \"name\", \"owner\", \"to\" from table(result_scan(LAST_QUERY_ID())) WHERE \"kind\" = 'OUTBOUND'")
	if err != nil {
		return fmt.Errorf("query snowflake shares with filter: %w", err)
	}

	defer rows.Close()

	var shareEntities []snowflake.ShareEntity

	err = scan.Rows(&shareEntities, rows)
	if err != nil {
		return fmt.Errorf("scan snowflake shares: %w", err)
	}

	for _, share := range shareEntities {
		if share.Owner == currentRole {
			fmt.Printf("drop share %q\n", share.Name)
			err = dropShare(db, share.Name)
			if err != nil {
				return fmt.Errorf("drop share %s: %w", share.Name, err)
			}
		}
	}

	return nil
}

func loadRoles(db *sql.DB) ([]snowflake.RoleEntity, error) {
	rows, err := db.Query("SHOW ROLES")
	if err != nil {
		return nil, fmt.Errorf("query snowflake roles: %w", err)
	}

	defer rows.Close()

	var roleEntities []snowflake.RoleEntity

	err = scan.Rows(&roleEntities, rows)
	if err != nil {
		return nil, fmt.Errorf("scan snowflake roles: %w", err)
	}

	return roleEntities, nil
}

type DbEntity struct {
	Name    string  `db:"name"`
	Comment *string `db:"comment"`
	Owner   string  `db:"owner"`
}

func loadDatabases(db *sql.DB) ([]DbEntity, error) {
	rows, err := db.Query("SHOW DATABASES")
	if err != nil {
		return nil, fmt.Errorf("query snowflake databases: %w", err)
	}

	defer rows.Close()

	var dbEntities []DbEntity

	err = scan.Rows(&dbEntities, rows)
	if err != nil {
		return nil, fmt.Errorf("scan snowflake databases: %w", err)
	}

	return dbEntities, nil
}

func loadDatabaseRoles(db *sql.DB, database string) ([]snowflake.RoleEntity, error) {
	query := common.FormatQuery("SHOW DATABASE ROLES IN DATABASE %s", database)

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query snowflake database roles: %w (%s)", err, query)
	}

	defer rows.Close()

	var roleEntities []snowflake.RoleEntity

	err = scan.Rows(&roleEntities, rows)
	if err != nil {
		return nil, fmt.Errorf("scan snowflake database roles: %w", err)
	}

	return roleEntities, nil
}

func dropRole(db *sql.DB, role snowflake.RoleEntity, database *string) error {
	if !nonDryRun {
		return nil
	}

	databaseRole := ""
	roleName := common.FormatQuery("%s", role.Name)

	if role.Owner != sfRole {
		_, err := db.Exec("GRANT OWNERSHIP ON ROLE " + roleName + " TO " + sfRole + " REVOKE CURRENT GRANTS")
		if err != nil {
			return fmt.Errorf("grant ownership on role %s: %w", role.Name, err)
		}
	}

	if database != nil {
		databaseRole = "DATABASE"
		roleName = common.FormatQuery("%s.%s", *database, role.Name)
	}

	_, err := db.Exec(fmt.Sprintf("DROP %s ROLE IF EXISTS %s", databaseRole, roleName))
	if err != nil {
		return fmt.Errorf("drop role %s: %w", role.Name, err)
	}

	return nil
}

func dropShare(db *sql.DB, share string) error {
	if !nonDryRun {
		return nil
	}

	_, err := db.Exec(fmt.Sprintf("DROP SHARE IF EXISTS %s", share))
	if err != nil {
		return fmt.Errorf("drop share %s: %w", share, err)
	}

	return nil
}

func main() {
	flag.StringVar(&sfAccount, "sfAccount", "", "Snowflake account")
	flag.StringVar(&sfOrganization, "sfOrganization", "", "Snowflake organization")
	flag.StringVar(&sfUser, "sfUser", "", "Snowflake user")
	flag.StringVar(&sfPassword, "sfPassword", "", "Snowflake password")
	flag.StringVar(&sfPrivateKeyFile, "sfPrivateKey", "", "Snowflake private key file path")
	flag.BoolVar(&nonDryRun, "drop", false, "Execute drop roles. If not set or false a dry run will be executed.")
	flag.Parse()

	if sfAccount == "" || sfUser == "" || (sfPassword == "" && sfPrivateKeyFile == "") {
		panic("Missing required arguments")
	}

	if nonDryRun {
		fmt.Printf("All non system roles will be dropped\n")
	} else {
		fmt.Printf("Executing dry run. No roles will be dropped\n")
	}

	err := dropAllRoles()
	if err != nil {
		panic(err)
	}
}
