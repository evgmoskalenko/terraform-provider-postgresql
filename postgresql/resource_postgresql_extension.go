package postgresql

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/errwrap"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/lib/pq"
)

const (
	extNameAttr     = "name"
	extDatabaseAttr = "database"
	extSchemaAttr   = "schema"
	extVersionAttr  = "version"
)

func resourcePostgreSQLExtension() *schema.Resource {
	return &schema.Resource{
		Create: resourcePostgreSQLExtensionCreate,
		Read:   resourcePostgreSQLExtensionRead,
		Update: resourcePostgreSQLExtensionUpdate,
		Delete: resourcePostgreSQLExtensionDelete,
		Exists: resourcePostgreSQLExtensionExists,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			extNameAttr: {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			extDatabaseAttr: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The database to create the extension in",
			},
			extSchemaAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Default:     "public",
				Description: "Sets the schema of an extension",
			},
			extVersionAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				Description: "Sets the version number of the extension",
			},
		},
	}
}

func resourcePostgreSQLExtensionCreate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)
	c.catalogLock.Lock()
	defer c.catalogLock.Unlock()

	extName := d.Get(extNameAttr).(string)

	b := bytes.NewBufferString("CREATE EXTENSION ")
	fmt.Fprint(b, pq.QuoteIdentifier(extName))

	if v, ok := d.GetOk(extSchemaAttr); ok {
		fmt.Fprint(b, " SCHEMA ", pq.QuoteIdentifier(v.(string)))
	}

	if v, ok := d.GetOk(extVersionAttr); ok {
		fmt.Fprint(b, " VERSION ", pq.QuoteIdentifier(v.(string)))
	}

	sql := b.String()

	db, err := getDBConnection(c, d.Get("database").(string))
	if err != nil {
		return err
	}
	if _, err := db.Exec(sql); err != nil {
		return errwrap.Wrapf("Error creating extension: {{err}}", err)
	}

	d.SetId(generateExtID(d))

	return resourcePostgreSQLExtensionReadImpl(d, meta)
}

func resourcePostgreSQLExtensionExists(d *schema.ResourceData, meta interface{}) (bool, error) {
	c := meta.(*Client)
	c.catalogLock.Lock()
	defer c.catalogLock.Unlock()

	database, extName, err := getExtNames(d)
	if err != nil {
		return false, err
	}

	// Check the database exists
	exists, err := dbExists(c.DB(), database)
	if err != nil || !exists {
		return false, err
	}

	db, err := getDBConnection(c, database)
	if err != nil {
		return false, err
	}

	query := "SELECT extname FROM pg_catalog.pg_extension WHERE extname = $1"
	err = db.QueryRow(query, extName).Scan(&extName)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	}

	return true, nil
}

func resourcePostgreSQLExtensionRead(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)
	c.catalogLock.RLock()
	defer c.catalogLock.RUnlock()

	return resourcePostgreSQLExtensionReadImpl(d, meta)
}

func resourcePostgreSQLExtensionReadImpl(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)

	database, extName, err := getExtNames(d)
	if err != nil {
		return err
	}

	db, err := getDBConnection(c, database)
	if err != nil {
		return err
	}

	var extSchema, extVersion string
	query := `SELECT n.nspname, e.extversion ` +
		`FROM pg_catalog.pg_extension e, pg_catalog.pg_namespace n ` +
		`WHERE n.oid = e.extnamespace AND e.extname = $1`
	err = db.QueryRow(query, extName).Scan(&extSchema, &extVersion)
	switch {
	case err == sql.ErrNoRows:
		log.Printf("[WARN] PostgreSQL extension (%s) not found", d.Id())
		d.SetId("")
		return nil
	case err != nil:
		return errwrap.Wrapf("Error reading extension: {{err}}", err)
	}

	d.Set(extNameAttr, extName)
	d.Set(extDatabaseAttr, database)
	d.Set(extSchemaAttr, extSchema)
	d.Set(extVersionAttr, extVersion)
	d.SetId(generateExtID(d))

	return nil
}

func resourcePostgreSQLExtensionDelete(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)
	c.catalogLock.Lock()
	defer c.catalogLock.Unlock()

	database, extName, err := getExtNames(d)
	if err != nil {
		return err
	}
	db, err := getDBConnection(c, database)
	if err != nil {
		return err
	}

	sql := fmt.Sprintf("DROP EXTENSION %s", pq.QuoteIdentifier(extName))
	if _, err := db.Exec(sql); err != nil {
		return errwrap.Wrapf("Error deleting extension: {{err}}", err)
	}

	return nil
}

func resourcePostgreSQLExtensionUpdate(d *schema.ResourceData, meta interface{}) error {
	c := meta.(*Client)
	c.catalogLock.Lock()
	defer c.catalogLock.Unlock()

	txn, err := startTransaction(c, d.Get(extDatabaseAttr).(string))
	if err != nil {
		return err
	}
	defer txn.Rollback()

	if err := setExtSchema(txn, d); err != nil {
		return err
	}

	if err := setExtVersion(txn, d); err != nil {
		return err
	}

	return resourcePostgreSQLExtensionReadImpl(d, meta)
}

func setExtSchema(txn *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(extSchemaAttr) {
		return nil
	}

	extName := d.Get(extNameAttr).(string)
	schema := d.Get(extSchemaAttr).(string)
	if schema == "" {
		return errors.New("schema name cannot be set to an empty string")
	}

	sql := fmt.Sprintf(
		"ALTER EXTENSION %s SET SCHEMA %s",
		pq.QuoteIdentifier(extName), pq.QuoteIdentifier(schema),
	)
	if _, err := txn.Exec(sql); err != nil {
		return errwrap.Wrapf("Error updating extension SCHEMA: {{err}}", err)
	}

	return nil
}

func setExtVersion(txn *sql.Tx, d *schema.ResourceData) error {
	if !d.HasChange(extVersionAttr) {
		return nil
	}

	name := d.Get(extNameAttr).(string)
	version := d.Get(extVersionAttr).(string)

	b := bytes.NewBufferString("ALTER EXTENSION ")
	fmt.Fprintf(b, "%s UPDATE", pq.QuoteIdentifier(name))

	// If version is not specified, it will update to the latest version
	if version != "" {
		fmt.Fprintf(b, " TO %s", pq.QuoteIdentifier(version))
	}

	sql := b.String()
	if _, err := txn.Exec(sql); err != nil {
		return errwrap.Wrapf("Error updating extension version: {{err}}", err)
	}

	return nil
}

func generateExtID(d *schema.ResourceData) string {
	return fmt.Sprintf("%s-%s", d.Get(extDatabaseAttr).(string), d.Get(extNameAttr).(string))
}

// getExtNames returns database and extension name. If we are importing this resource, they will be parsed
// from the resource ID (it will return an error if parsing failed) otherwise they will be simply
// get from the state.
func getExtNames(d *schema.ResourceData) (string, string, error) {
	var database, extName string

	database = d.Get(extDatabaseAttr).(string)
	extName = d.Get(extNameAttr).(string)

	// When importing, we have to parse the ID to find extension and database names.
	if extName == "" {
		parsed := strings.Split(d.Id(), "-")
		if len(parsed) != 2 {
			return "", "", fmt.Errorf("extension ID %s has not the expected format 'database-extension': %v", d.Id(), parsed)
		}
		database = parsed[0]
		extName = parsed[1]
	}
	return database, extName, nil
}
