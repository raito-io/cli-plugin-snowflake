package main

import (
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"os"

	"github.com/hashicorp/go-hclog"
	sf "github.com/snowflakedb/gosnowflake"
	"golang.org/x/exp/rand"

	"encoding/json"
	"fmt"

	"github.com/raito-io/cli-plugin-snowflake/snowflake"
)

var logger hclog.Logger

type UsageConfig struct {
	Password struct {
		Value string `json:"value"`
	} `json:"persona_password"`
	PersonaRsaPrivateKey struct {
		Value string `json:"value"`
	} `json:"persona_rsa_private_key"`
	Personas struct {
		Value []struct {
			Roles []string `json:"roles"`
			User  string   `json:"user"`
			Email string   `json:"email"`
		} `json:"value"`
	} `json:"personas"`
	SnowflakeDataBaseName struct {
		Value string `json:"value"`
	} `json:"snowflake_database_name"`
	SnowflakeTables struct {
		Value []string `json:"value"`
	} `json:"snowflake_tables"`
	SnowflakeAccount struct {
		Value string `json:"value"`
	} `json:"snowflake_account"`
	SnowflakeWarehouse struct {
		Value string `json:"value"`
	} `json:"snowflake_warehouse_name"`
}

func CreateUsage(config *UsageConfig) error {
	logger.Info(fmt.Sprintf("rsa private key length: %d", len(config.PersonaRsaPrivateKey.Value)))

	block, _ := pem.Decode([]byte(config.PersonaRsaPrivateKey.Value))

	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("parse rsa private key: %w", err)
	}

	for _, persona := range config.Personas.Value {
		logger.Info(fmt.Sprintf("Executing queries for %q", persona.User))

		for _, role := range persona.Roles {
			err = executeQueryUsage(config.SnowflakeAccount.Value, persona.User, role, config.Password.Value, key, config.SnowflakeDataBaseName.Value, config.SnowflakeWarehouse.Value, config.SnowflakeTables.Value)
			if err != nil {
				return fmt.Errorf("execute usage: %w", err)
			}
		}
	}

	return nil
}

func executeQueryUsage(account string, email string, role string, password string, rsaPrivateKey *rsa.PrivateKey, database string, warehouse string, tables []string) error {
	conn, err := openConnection(account, email, role, password, rsaPrivateKey, database, warehouse)
	if err != nil {
		return fmt.Errorf("open connection: %w", err)
	}

	defer conn.Close()

	for _, table := range tables {
		r := rand.Intn(10)
		for range r {
			query := fmt.Sprintf("SELECT * FROM %s LIMIT 1000", table)
			rows, err := conn.Query(query)
			if err != nil {
				logger.Info(fmt.Sprintf("Query %q execution failed: %s", query, err.Error()))
			} else {
				logger.Info(fmt.Sprintf("Query %q executed successfully", query))
				for rows.Next() {
					// Do nothng
				}
				rows.Close()
			}
		}
	}

	return nil
}

func openConnection(account string, username string, role string, password string, rsaPrivateKey *rsa.PrivateKey, database string, warehouse string) (*sql.DB, error) {
	dsn, err := sf.DSN(&sf.Config{
		Account:       account,
		User:          username,
		Database:      database,
		Password:      password,
		PrivateKey:    rsaPrivateKey,
		Role:          role,
		Warehouse:     warehouse,
		Application:   snowflake.ConnectionStringIdentifier,
		Authenticator: sf.AuthTypeJwt,
	})

	if err != nil {
		return nil, fmt.Errorf("snowflake DSN: %w", err)
	}

	logger.Info(fmt.Sprintf("Opening connection to DSN: %s", dsn))

	conn, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, fmt.Errorf("open snowflake: %w", err)
	}

	err = conn.Ping()
	if err != nil {
		return nil, fmt.Errorf("ping snowflake: %w", err)
	}

	return conn, nil
}

func main() {
	sf.CreateDefaultLogger()
	_ = sf.GetLogger().SetLogLevel("panic")

	logger = hclog.New(&hclog.LoggerOptions{Name: "usage-logger", Level: hclog.Info})

	info, err := os.Stdin.Stat()
	if err != nil {
		panic(err)
	}

	if info.Mode()&os.ModeCharDevice != 0 {
		fmt.Println("The command is intended to work with pipes.")
		return
	}

	dec := json.NewDecoder(os.Stdin)

	usageConfig := UsageConfig{}

	err = dec.Decode(&usageConfig)
	if err != nil {
		panic(err)
	}

	err = CreateUsage(&usageConfig)
	if err != nil {
		panic(err)
	}
}
