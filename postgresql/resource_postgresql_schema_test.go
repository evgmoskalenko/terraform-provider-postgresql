package postgresql

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/hashicorp/errwrap"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
)

func TestAccPostgresqlSchema_Basic(t *testing.T) {
	skipIfNotAcc(t)

	dbSuffix, teardown := setupTestDatabase(t, true, false, false)
	defer teardown()

	dbName, _ := getTestDBNames(dbSuffix)

	var testConfig = fmt.Sprintf(testSchemaConfigBasic, dbName, dbName, dbName)

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlSchemasDestroy(t, dbName, []string{"foo", "bar", "baz"}),
		Steps: []resource.TestStep{
			{
				Config: testConfig,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlSchemasExist(t, dbName, []string{"foo", "bar", "baz"}),
					resource.TestCheckResourceAttr("postgresql_role.role_all_without_grant", "name", "role_all_without_grant"),
					resource.TestCheckResourceAttr("postgresql_role.role_all_without_grant", "login", "true"),

					resource.TestCheckResourceAttr("postgresql_role.role_all_with_grant", "name", "role_all_with_grant"),

					resource.TestCheckResourceAttr("postgresql_schema.test1", "name", "foo"),

					resource.TestCheckResourceAttr("postgresql_schema.test2", "name", "bar"),
					resource.TestCheckResourceAttr("postgresql_schema.test2", "owner", "role_all_without_grant"),
					resource.TestCheckResourceAttr("postgresql_schema.test2", "if_not_exists", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test2", "policy.#", "1"),
					resource.TestCheckResourceAttr("postgresql_schema.test2", "policy.1948480595.create", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test2", "policy.1948480595.create_with_grant", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test2", "policy.1948480595.usage", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test2", "policy.1948480595.usage_with_grant", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test2", "policy.1948480595.role", "role_all_without_grant"),

					resource.TestCheckResourceAttr("postgresql_schema.test3", "name", "baz"),
					resource.TestCheckResourceAttr("postgresql_schema.test3", "owner", "role_all_without_grant"),
					resource.TestCheckResourceAttr("postgresql_schema.test3", "if_not_exists", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test3", "policy.#", "2"),
					resource.TestCheckResourceAttr("postgresql_schema.test3", "policy.1013320538.create_with_grant", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test3", "policy.1013320538.usage_with_grant", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test3", "policy.1013320538.role", "role_all_with_grant"),
					resource.TestCheckResourceAttr("postgresql_schema.test3", "policy.1948480595.create", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test3", "policy.1948480595.usage", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test3", "policy.1948480595.role", "role_all_without_grant"),
				),
			},
		},
	})
}

func TestAccPostgresqlSchema_AddPolicy(t *testing.T) {
	skipIfNotAcc(t)

	dbSuffix, teardown := setupTestDatabase(t, true, false, false)
	defer teardown()

	dbName, _ := getTestDBNames(dbSuffix)

	testConfig := fmt.Sprintf(testSchemaConfigAddPolicy, dbName)
	testConfigUpdate := fmt.Sprintf(testSchemaConfigAddPolicyUpdate, dbName)

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlSchemasDestroy(t, dbName, []string{"test4"}),
		Steps: []resource.TestStep{
			{
				Config: testConfig,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlSchemasExist(t, dbName, []string{"test4"}),

					resource.TestCheckResourceAttr("postgresql_role.all_without_grant_stay", "name", "all_without_grant_stay"),
					resource.TestCheckResourceAttr("postgresql_role.all_without_grant_drop", "name", "all_without_grant_drop"),
					resource.TestCheckResourceAttr("postgresql_role.policy_compose", "name", "policy_compose"),
					resource.TestCheckResourceAttr("postgresql_role.policy_move", "name", "policy_move"),

					resource.TestCheckResourceAttr("postgresql_role.all_with_grantstay", "name", "all_with_grantstay"),
					resource.TestCheckResourceAttr("postgresql_role.all_with_grantdrop", "name", "all_with_grantdrop"),

					resource.TestCheckResourceAttr("postgresql_schema.test4", "name", "test4"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "owner", "all_without_grant_stay"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.#", "7"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.108605972.create", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.108605972.create_with_grant", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.108605972.role", "all_with_grantstay"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.108605972.usage", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.108605972.usage_with_grant", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.1417738359.create", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.1417738359.create_with_grant", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.1417738359.role", "policy_move"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.1417738359.usage", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.1417738359.usage_with_grant", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.1762357194.create", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.1762357194.create_with_grant", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.1762357194.role", "all_without_grant_drop"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.1762357194.usage", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.1762357194.usage_with_grant", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.2524457447.create", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.2524457447.create_with_grant", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.2524457447.role", "all_without_grant_stay"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.2524457447.usage", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.2524457447.usage_with_grant", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.3959936977.create", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.3959936977.create_with_grant", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.3959936977.role", "policy_compose"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.3959936977.usage", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.3959936977.usage_with_grant", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.4178211897.create", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.4178211897.create_with_grant", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.4178211897.role", "all_with_grantdrop"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.4178211897.usage", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.4178211897.usage_with_grant", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.815478369.create", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.815478369.create_with_grant", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.815478369.role", "policy_compose"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.815478369.usage", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.815478369.usage_with_grant", "false"),
				),
			},
			{
				Config: testConfigUpdate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlSchemasExist(t, dbName, []string{"test4"}),
					resource.TestCheckResourceAttr("postgresql_role.all_without_grant_stay", "name", "all_without_grant_stay"),
					resource.TestCheckResourceAttr("postgresql_role.all_without_grant_drop", "name", "all_without_grant_drop"),
					resource.TestCheckResourceAttr("postgresql_role.policy_compose", "name", "policy_compose"),
					resource.TestCheckResourceAttr("postgresql_role.policy_move", "name", "policy_move"),

					resource.TestCheckResourceAttr("postgresql_role.all_with_grantstay", "name", "all_with_grantstay"),

					resource.TestCheckResourceAttr("postgresql_schema.test4", "name", "test4"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "owner", "all_without_grant_stay"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.#", "6"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.108605972.create", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.108605972.create_with_grant", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.108605972.role", "all_with_grantstay"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.108605972.usage", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.108605972.usage_with_grant", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.2524457447.create", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.2524457447.create_with_grant", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.2524457447.role", "all_without_grant_stay"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.2524457447.usage", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.2524457447.usage_with_grant", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.3831594020.create", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.3831594020.create_with_grant", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.3831594020.role", "policy_move"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.3831594020.usage", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.3831594020.usage_with_grant", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.3959936977.create", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.3959936977.create_with_grant", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.3959936977.role", "policy_compose"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.3959936977.usage", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.3959936977.usage_with_grant", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.468685299.create", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.468685299.create_with_grant", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.468685299.role", "policy_new"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.468685299.usage", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.468685299.usage_with_grant", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.815478369.create", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.815478369.create_with_grant", "false"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.815478369.role", "policy_compose"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.815478369.usage", "true"),
					resource.TestCheckResourceAttr("postgresql_schema.test4", "policy.815478369.usage_with_grant", "false"),
				),
			},
		},
	})
}

func testAccCheckPostgresqlSchemasDestroy(t *testing.T, dbName string, schemas []string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		for _, schemaName := range schemas {
			exists, err := checkSchemaExists(t, dbName, schemaName)
			if err != nil {
				return fmt.Errorf("Error checking schema %s", err)
			}

			if exists {
				return fmt.Errorf("Schema still exists after destroy")
			}
		}
		return nil
	}
}

func testAccCheckPostgresqlSchemasExist(t *testing.T, dbName string, schemas []string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		for _, schemaName := range schemas {
			exists, err := checkSchemaExists(t, dbName, schemaName)
			if err != nil {
				return fmt.Errorf("Error checking schema %s", err)
			}

			if !exists {
				return fmt.Errorf("Schema not found")
			}
		}
		return nil
	}
}

func checkSchemaExists(t *testing.T, dbName string, schemaName string) (bool, error) {
	config := getTestConfig(t)

	db, err := sql.Open("postgres", config.connStr(dbName))
	if err != nil {
		t.Fatalf("could not open connection pool for db %s: %v", dbName, err)
	}
	defer db.Close()

	var _rez string
	if err := db.QueryRow("SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname=$1", schemaName).Scan(&_rez); err != nil {
		switch {
		case err == sql.ErrNoRows:
			return false, nil
		default:
			return false, errwrap.Wrapf("error reading info about schema: {{err}}", err)
		}
	}

	return true, nil
}

var testSchemaConfigBasic = `
resource "postgresql_role" "role_all_without_grant" {
  name = "role_all_without_grant"
  login = true
}

resource "postgresql_role" "role_all_with_grant" {
  name = "role_all_with_grant"
}

resource "postgresql_schema" "test1" {
  name = "foo"
  database = "%s"
}

resource "postgresql_schema" "test2" {
  name = "bar"
  database = "%s"
  owner = "${postgresql_role.role_all_without_grant.name}"
  if_not_exists = false

  policy {
    create = true
    usage = true
    role = "${postgresql_role.role_all_without_grant.name}"
  }
}

resource "postgresql_schema" "test3" {
  name = "baz"
  database = "%s"
  owner = "${postgresql_role.role_all_without_grant.name}"
  if_not_exists = true

  policy {
    create_with_grant = true
    usage_with_grant = true
    role = "${postgresql_role.role_all_with_grant.name}"
  }

  policy {
    create = true
    usage = true
    role = "${postgresql_role.role_all_without_grant.name}"
  }
}`

var testSchemaConfigAddPolicy = `
resource "postgresql_role" "all_without_grant_stay" {
  name = "all_without_grant_stay"
}

resource "postgresql_role" "all_without_grant_drop" {
  name = "all_without_grant_drop"
}

resource "postgresql_role" "policy_compose" {
  name = "policy_compose"
}

resource "postgresql_role" "policy_move" {
  name = "policy_move"
}

resource "postgresql_role" "all_with_grantstay" {
  name = "all_with_grantstay"
}

resource "postgresql_role" "all_with_grantdrop" {
  name = "all_with_grantdrop"
}

resource "postgresql_schema" "test4" {
  name = "test4"
  database = "%s"
  owner = "${postgresql_role.all_without_grant_stay.name}"

  policy {
    create = true
    usage = true
    role = "${postgresql_role.all_without_grant_stay.name}"
  }

  policy {
    create = true
    usage = true
    role = "${postgresql_role.all_without_grant_drop.name}"
  }

  policy {
    create = true
    usage = true
    role = "${postgresql_role.policy_compose.name}"
  }

  policy {
    create = true
    usage = true
    role = "${postgresql_role.policy_move.name}"
  }

  policy {
    create_with_grant = true
    usage_with_grant = true
    role = "${postgresql_role.all_with_grantstay.name}"
  }

  policy {
    create_with_grant = true
    usage_with_grant = true
    role = "${postgresql_role.all_with_grantdrop.name}"
  }

  policy {
    create_with_grant = true
    usage_with_grant = true
    role = "${postgresql_role.policy_compose.name}"
  }
}
`

var testSchemaConfigAddPolicyUpdate = `
resource "postgresql_role" "all_without_grant_stay" {
  name = "all_without_grant_stay"
}

resource "postgresql_role" "all_without_grant_drop" {
  name = "all_without_grant_drop"
}

resource "postgresql_role" "policy_compose" {
  name = "policy_compose"
}

resource "postgresql_role" "policy_move" {
  name = "policy_move"
}

resource "postgresql_role" "all_with_grantstay" {
  name = "all_with_grantstay"
}

resource "postgresql_role" "policy_new" {
  name = "policy_new"
}

resource "postgresql_schema" "test4" {
  name = "test4"
  database = "%s"
  owner    = "${postgresql_role.all_without_grant_stay.name}"

  policy {
    create = true
    usage = true
    role = "${postgresql_role.all_without_grant_stay.name}"
  }

  policy {
    create = true
    usage = true
    role = "${postgresql_role.policy_compose.name}"
  }

  policy {
    create_with_grant = true
    usage_with_grant = true
    role = "${postgresql_role.all_with_grantstay.name}"
  }

  policy {
    create_with_grant = true
    usage_with_grant = true
    role = "${postgresql_role.policy_compose.name}"
  }

  policy {
    create_with_grant = true
    usage_with_grant = true
    role = "${postgresql_role.policy_move.name}"
  }

  policy {
    create = true
    usage = true
    role = "${postgresql_role.policy_new.name}"
  }
}
`
