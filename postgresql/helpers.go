package postgresql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/hashicorp/errwrap"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/lib/pq"
)

// pqQuoteLiteral returns a string literal safe for inclusion in a PostgreSQL
// query as a parameter.  The resulting string still needs to be wrapped in
// single quotes in SQL (i.e. fmt.Sprintf(`'%s'`, pqQuoteLiteral("str"))).  See
// quote_literal_internal() in postgresql/backend/utils/adt/quote.c:77.
func pqQuoteLiteral(in string) string {
	in = strings.Replace(in, `\`, `\\`, -1)
	in = strings.Replace(in, `'`, `''`, -1)
	return in
}

func validateConnLimit(v interface{}, key string) (warnings []string, errors []error) {
	value := v.(int)
	if value < -1 {
		errors = append(errors, fmt.Errorf("%s can not be less than -1", key))
	}
	return
}

func sliceContainsStr(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

// allowedPrivileges is the list of privileges allowed per object types in Postgres.
// see: https://www.postgresql.org/docs/current/sql-grant.html
var allowedPrivileges = map[string][]string{
	"table":    []string{"ALL", "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"},
	"sequence": []string{"ALL", "USAGE", "SELECT", "UPDATE"},
}

// validatePrivileges checks that privileges to apply are allowed for this object type.
func validatePrivileges(objectType string, privileges []interface{}) error {
	allowed, ok := allowedPrivileges[objectType]
	if !ok {
		return fmt.Errorf("unknown object type %s", objectType)
	}

	for _, priv := range privileges {
		if !sliceContainsStr(allowed, priv.(string)) {
			return fmt.Errorf("%s is not an allowed privilege for object type %s", priv, objectType)
		}
	}
	return nil
}

func pgArrayToSet(arr pq.ByteaArray) *schema.Set {
	s := make([]interface{}, len(arr))
	for i, v := range arr {
		s[i] = string(v)
	}
	return schema.NewSet(schema.HashString, s)
}

// startTransaction starts a new DB transaction on the specified database.
// If the database is specified and different from the one configured in the provider,
// it will create a new connection pool if needed.
func startTransaction(client *Client, database string) (*sql.Tx, error) {
	if database != "" && database != client.databaseName {
		var err error
		client, err = client.config.NewClient(database)
		if err != nil {
			return nil, err
		}
	}
	db := client.DB()
	txn, err := db.Begin()
	if err != nil {
		return nil, errwrap.Wrapf("could not start transaction: {{err}}", err)
	}

	return txn, nil
}

func dbExists(txn *sql.Tx, dbname string) (bool, error) {
	err := txn.QueryRow("SELECT datname FROM pg_database WHERE datname=$1", dbname).Scan(&dbname)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, errwrap.Wrapf("could not check if database exists: {{err}}", err)
	}

	return true, nil
}

func roleExists(txn *sql.Tx, rolname string) (bool, error) {
	err := txn.QueryRow("SELECT 1 FROM pg_roles WHERE rolname=$1", rolname).Scan(&rolname)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, errwrap.Wrapf("could not check if role exists: {{err}}", err)
	}

	return true, nil
}

func schemaExists(txn *sql.Tx, schemaname string) (bool, error) {
	err := txn.QueryRow("SELECT 1 FROM pg_namespace WHERE nspname=$1", schemaname).Scan(&schemaname)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, errwrap.Wrapf("could not check if schema exists: {{err}}", err)
	}

	return true, nil
}
