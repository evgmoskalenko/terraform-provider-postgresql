package postgresql

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
)

const (
	testExtName = "pg_trgm"
)

func TestAccPostgresqlExtension_Basic(t *testing.T) {
	skipIfNotAcc(t)
	// We want to test only the extension part in the terraform config
	// so it's better to manually create the database
	// (e.g.: it allows us to really test the destroy of an extension without
	//  the whole db being dropped by Terraform)
	dbSuffix, teardown := setupTestDatabase(t, true, false, false)
	defer teardown()

	dbName, _ := getTestDBNames(dbSuffix)

	var testConfig = fmt.Sprintf(`
resource "postgresql_extension" "myextension" {
  name     = "%s"
  database = "%s"
}
	`, testExtName, dbName)

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlExtensionDestroy(t, dbName, testExtName),
		Steps: []resource.TestStep{
			{
				Config: testConfig,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlExtensionExists(t, dbName, testExtName),
					resource.TestCheckResourceAttr(
						"postgresql_extension.myextension", "name", "pg_trgm"),
					resource.TestCheckResourceAttr(
						"postgresql_extension.myextension", "schema", "public"),

					// NOTE(sean): The version number drifts.  PG 9.6 ships with pg_trgm
					// version 1.3 and PG 9.2 ships with pg_trgm 1.0.
					resource.TestCheckResourceAttrSet(
						"postgresql_extension.myextension", "version"),
				),
			},
		},
	})
}

func TestAccPostgresqlExtension_SchemaRename(t *testing.T) {
	skipIfNotAcc(t)

	dbSuffix, teardown := setupTestDatabase(t, true, false, false)
	defer teardown()

	dbName, _ := getTestDBNames(dbSuffix)

	var testConfig = fmt.Sprintf(`
resource "postgresql_schema" "ext1foo" {
  name = "foo"
  database = "%s"
}

resource "postgresql_extension" "ext1trgm" {
  name = "%s"
  database = "%s"
  schema = "${postgresql_schema.ext1foo.name}"
}
`, dbName, testExtName, dbName)

	var testConfigUpdate = fmt.Sprintf(`
resource "postgresql_schema" "ext1foo" {
  name = "bar"
  database = "%s"
}

resource "postgresql_extension" "ext1trgm" {
  name = "%s"
  database = "%s"
  schema = "${postgresql_schema.ext1foo.name}"
}
`, dbName, testExtName, dbName)

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlExtensionDestroy(t, dbName, testExtName),
		Steps: []resource.TestStep{
			{
				Config: testConfig,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlExtensionExists(t, dbName, testExtName),
					resource.TestCheckResourceAttr(
						"postgresql_schema.ext1foo", "name", "foo"),
					resource.TestCheckResourceAttr(
						"postgresql_extension.ext1trgm", "name", "pg_trgm"),
					resource.TestCheckResourceAttr(
						"postgresql_extension.ext1trgm", "name", "pg_trgm"),
					resource.TestCheckResourceAttr(
						"postgresql_extension.ext1trgm", "schema", "foo"),
				),
			},
			{
				Config: testConfigUpdate,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr(
						"postgresql_schema.ext1foo", "name", "bar"),
					resource.TestCheckResourceAttr(
						"postgresql_extension.ext1trgm", "name", "pg_trgm"),
					resource.TestCheckResourceAttr(
						"postgresql_extension.ext1trgm", "schema", "bar"),
				),
			},
		},
	})
}

func testAccCheckPostgresqlExtensionDestroy(t *testing.T, dbName, extName string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		exists, err := checkExtensionExists(t, dbName, extName)
		if err != nil {
			return err
		}

		if exists {
			return fmt.Errorf("Extension is not destroyed")
		}

		return nil
	}
}

func testAccCheckPostgresqlExtensionExists(t *testing.T, dbName, extName string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		exists, err := checkExtensionExists(t, dbName, extName)
		if err != nil {
			return err
		}

		if !exists {
			return fmt.Errorf("Extension not found")
		}

		return nil
	}
}

func checkExtensionExists(t *testing.T, dbName, extName string) (bool, error) {
	config := getTestConfig(t)

	db, err := sql.Open("postgres", config.connStr(dbName))
	if err != nil {
		t.Fatalf("could not open connection pool for db %s: %v", dbName, err)
	}
	defer db.Close()

	var _rez bool
	err = db.QueryRow("SELECT TRUE from pg_catalog.pg_extension d WHERE extname=$1", extName).Scan(&_rez)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("Error reading info about extension: %s", err)
	}

	return true, nil
}
