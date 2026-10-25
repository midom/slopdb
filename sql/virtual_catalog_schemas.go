package sql

// Layouts are defined once for Describe and row construction.
var postgresCatalogSchemas = map[string]string{
	"pg_namespace":                         "oid:o nspname nspowner:o nspacl",
	"information_schema.schemata":          "catalog_name schema_name schema_owner default_character_set_catalog default_character_set_schema default_character_set_name sql_path",
	"pg_type":                              "oid:o typname typnamespace:o typowner:o typlen:i typbyval:b typtype typcategory typispreferred:b typisdefined:b typrelid:o typelem:o typarray:o typbasetype:o typtypmod:i typnotnull:b typdefault",
	"pg_am":                                "oid:o amname amtype",
	"pg_database":                          "oid:o datname datdba:o encoding:i datistemplate:b datallowconn:b datconnlimit:i datcollate datctype",
	"pg_class":                             "oid:o relname relnamespace:o reltype:o reloftype:o relowner:o relam:o relfilenode:o reltablespace:o relpages:i reltuples:r relallvisible:i reltoastrelid:o relhasindex:b relisshared:b relpersistence relkind relnatts:i relchecks:i relhasrules:b relhastriggers:b relhassubclass:b relrowsecurity:b relforcerowsecurity:b relispopulated:b relreplident relispartition:b relrewrite:o relacl reloptions",
	"pg_attribute":                         "attrelid:o attname atttypid:o attstattarget:i attlen:i attnum:i attndims:i attcacheoff:i atttypmod:i attbyval:b attstorage attalign attnotnull:b atthasdef:b atthasmissing:b attidentity attgenerated attisdropped:b attislocal:b attinhcount:i attcollation:o attacl attoptions attfdwoptions attmissingval",
	"pg_attrdef":                           "oid:o adrelid:o adnum:i adbin",
	"pg_index":                             "indexrelid:o indrelid:o indnatts:i indnkeyatts:i indisunique:b indnullsnotdistinct:b indisprimary:b indisexclusion:b indimmediate:b indisclustered:b indisvalid:b indcheckxmin:b indisready:b indislive:b indisreplident:b indkey indexprs indpred",
	"pg_tables":                            "schemaname tablename tableowner tablespace hasindexes:b hasrules:b hastriggers:b rowsecurity:b",
	"information_schema.tables":            "table_catalog table_schema table_name table_type self_referencing_column_name reference_generation user_defined_type_catalog user_defined_type_schema user_defined_type_name is_insertable_into is_typed commit_action",
	"pg_indexes":                           "schemaname tablename indexname tablespace indexdef",
	"pg_views":                             "schemaname viewname viewowner definition",
	"information_schema.views":             "table_catalog table_schema table_name view_definition check_option is_updatable is_insertable_into is_trigger_updatable is_trigger_deletable is_trigger_insertable_into",
	"information_schema.columns":           "table_catalog table_schema table_name column_name ordinal_position:i column_default is_nullable data_type character_maximum_length:i character_octet_length:i numeric_precision:i numeric_precision_radix:i numeric_scale:i datetime_precision:i interval_type interval_precision:i character_set_catalog character_set_schema character_set_name collation_catalog collation_schema collation_name domain_catalog domain_schema domain_name udt_catalog udt_schema udt_name scope_catalog scope_schema scope_name maximum_cardinality:i dtd_identifier is_self_referencing is_identity identity_generation identity_start identity_increment identity_maximum identity_minimum identity_cycle is_generated generation_expression is_updatable",
	"pg_constraint":                        "oid:o conname connamespace:o contype condeferrable:b condeferred:b convalidated:b conrelid:o contypid:o conindid:o conparentid:o confrelid:o confupdtype confdeltype confmatchtype conislocal:b coninhcount:i connoinherit:b conkey confkey conbin",
	"information_schema.table_constraints": "constraint_catalog constraint_schema constraint_name table_catalog table_schema table_name constraint_type is_deferrable initially_deferred enforced nulls_distinct",
	"information_schema.key_column_usage":  "constraint_catalog constraint_schema constraint_name table_catalog table_schema table_name column_name ordinal_position:i position_in_unique_constraint:i",
	"information_schema.referential_constraints": "constraint_catalog constraint_schema constraint_name unique_constraint_catalog unique_constraint_schema unique_constraint_name match_option update_rule delete_rule",
}

func pgCatalogFor(name string) *ResultSet { return pgCatalog(postgresCatalogSchemas[name]) }
