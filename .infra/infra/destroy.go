package main

import (
	"database/sql"
	"flag"
	"fmt"

	"github.com/blockloop/scan"
	"github.com/raito-io/golang-set/set"
	sf "github.com/snowflakedb/gosnowflake"

	"github.com/raito-io/cli-plugin-snowflake/snowflake"
)

var systemRoles = set.Set[string]{"ORGADMIN": {}, "ACCOUNTADMIN": {}, "SECURITYADMIN": {}, "USERADMIN": {}, "SYSADMIN": {}, "PUBLIC": {}}

var (
	sfAccount, sfUser, sfPassword string
	nonDryRun                     bool
)

func dropAllRoles() error {
	dsn, err := sf.DSN(&sf.Config{
		Account:  sfAccount,
		User:     sfUser,
		Password: sfPassword,
		Role:     "ACCOUNTADMIN",
	})

	if err != nil {
		return fmt.Errorf("snowflake dsn: %w", err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return fmt.Errorf("open snowflake: %w", err)
	}

	defer db.Close()

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
		err = dropRole(db, existingRole)
		if err != nil {
			return fmt.Errorf("drop role %s: %w", existingRole.Name, err)
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

func dropRole(db *sql.DB, role snowflake.RoleEntity) error {
	if !nonDryRun {
		return nil
	}

	_, err := db.Exec(fmt.Sprintf("DROP ROLE IF EXISTS %s", role.Name))
	if err != nil {
		return fmt.Errorf("drop role %s: %w", role.Name, err)
	}

	return nil
}

func main() {
	flag.StringVar(&sfAccount, "sfAccount", "", "Snowflake account")
	flag.StringVar(&sfUser, "sfUser", "", "Snowflake user")
	flag.StringVar(&sfPassword, "sfPassword", "", "Snowflake password")
	flag.BoolVar(&nonDryRun, "drop", false, "Execute drop roles. If not set or false a dry run will be executed.")
	flag.Parse()

	if sfAccount == "" || sfUser == "" || sfPassword == "" {
		fmt.Println("Missing required arguments")
		return
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
