package sql

import "strings"

// Layouts are defined once for Describe and row construction.
var postgresCatalogSchemas = map[string]string{
	"pg_stat_activity": "datid:o datname:n pid:j leader_pid:j usesysid:o usename:n application_name client_addr client_hostname client_port:j backend_start:z xact_start:z query_start:z state_change:z wait_event_type wait_event state backend_xid backend_xmin query_id:i query backend_type slopdb_session_id:u slopdb_backend_id:u slopdb_protocol slopdb_wait_kind slopdb_wait_resource",
	"pg_namespace":     "oid:o nspname nspowner:o nspacl",
	"pg_type":          "oid:o typname typnamespace:o typowner:o typlen:i typbyval:b typtype typcategory typispreferred:b typisdefined:b typrelid:o typelem:o typarray:o typbasetype:o typtypmod:i typnotnull:b typdefault",
	"pg_am":            "oid:o amname amtype",
	"pg_database":      "oid:o datname datdba:o encoding:i datistemplate:b datallowconn:b datconnlimit:i datcollate datctype",
	"pg_class":         "oid:o relname relnamespace:o reltype:o reloftype:o relowner:o relam:o relfilenode:o reltablespace:o relpages:i reltuples:r relallvisible:i reltoastrelid:o relhasindex:b relisshared:b relpersistence relkind relnatts:i relchecks:i relhasrules:b relhastriggers:b relhassubclass:b relrowsecurity:b relforcerowsecurity:b relispopulated:b relreplident relispartition:b relrewrite:o relacl reloptions",
	"pg_attribute":     "attrelid:o attname atttypid:o attstattarget:i attlen:i attnum:i attndims:i attcacheoff:i atttypmod:i attbyval:b attstorage attalign attnotnull:b atthasdef:b atthasmissing:b attidentity attgenerated attisdropped:b attislocal:b attinhcount:i attcollation:o attacl attoptions attfdwoptions attmissingval",
	"pg_attrdef":       "oid:o adrelid:o adnum:i adbin",
	"pg_index":         "indexrelid:o indrelid:o indnatts:i indnkeyatts:i indisunique:b indnullsnotdistinct:b indisprimary:b indisexclusion:b indimmediate:b indisclustered:b indisvalid:b indcheckxmin:b indisready:b indislive:b indisreplident:b indkey indexprs indpred",
	"pg_tables":        "schemaname tablename tableowner tablespace hasindexes:b hasrules:b hastriggers:b rowsecurity:b",
	"pg_indexes":       "schemaname tablename indexname tablespace indexdef",
	"pg_views":         "schemaname viewname viewowner definition",
	"pg_constraint":    "oid:o conname connamespace:o contype condeferrable:b condeferred:b convalidated:b conrelid:o contypid:o conindid:o conparentid:o confrelid:o confupdtype confdeltype confmatchtype conislocal:b coninhcount:i connoinherit:b conkey confkey conbin",
}

func pgCatalogFor(name string) *ResultSet {
	if columns, known := informationSchemaColumns(name); strings.HasPrefix(name, "information_schema.") && known {
		return &ResultSet{Columns: columns}
	}
	return pgCatalog(postgresCatalogSchemas[name])
}
