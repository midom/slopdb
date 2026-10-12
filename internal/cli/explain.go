package cli

import (
	"fmt"
	"os"
	"strings"

	"slopdb/db"
	sqldb "slopdb/sql"
)

func queryExplanationSQL(query string) (string, error) {
	statements, err := sqldb.Parse(query)
	if err != nil {
		return "", err
	}
	if len(statements) != 1 {
		return "", fmt.Errorf(".explain requires one SELECT statement")
	}
	statement := statements[0]
	wrapped := false
	if explain, ok := statement.(*sqldb.ExplainStmt); ok {
		if explain.Analyze {
			return "", fmt.Errorf(".explain shows a plan; run EXPLAIN ANALYZE SELECT explicitly to measure execution")
		}
		wrapped, statement = true, explain.Stmt
	}
	switch statement.(type) {
	case *sqldb.SelectStmt, *sqldb.UnionStmt:
	default:
		return "", fmt.Errorf(".explain requires one SELECT statement")
	}
	if wrapped {
		return query, nil
	}
	return "EXPLAIN " + query, nil
}

func printQueryExplanation(query string) {
	explain, err := queryExplanationSQL(query)
	if err != nil {
		queryExplanationError(err)
		return
	}
	result, err := currentDB.Exec(explain)
	if err != nil {
		queryExplanationError(err)
		return
	}
	cliPrint(formatQueryExplanation(result))
}

func queryExplanationError(err error) {
	fmt.Fprintf(os.Stderr, "Explain error: %v\n", err)
	if cliCfg.bail {
		exitCLI(1)
	}
}

func formatQueryExplanation(result *db.ExecResult) string {
	var b strings.Builder
	b.WriteString("Estimated query plan (query not executed):\n")
	var scan, index, sort, join bool
	for _, row := range result.Rows {
		opcode, detail, comment := row["opcode"].Text, row["p4"].Text, row["comment"].Text
		switch opcode {
		case "OpenRead", "OpenIndex", "Rewind", "PointLookup", "SeekGE", "SeekLT", "MultiGet", "JoinPlan", "JoinAccess", "Filter", "SorterSort", "Limit":
			fmt.Fprintf(&b, "  %s", opcode)
			if detail != "" {
				fmt.Fprintf(&b, ": %s", detail)
			}
			if comment != "" {
				fmt.Fprintf(&b, " (%s)", comment)
			}
			if opcode == "Limit" {
				fmt.Fprintf(&b, ": %d rows", row["p2"].Int)
			}
			b.WriteByte('\n')
		}
		scan = scan || opcode == "Rewind" || (opcode == "JoinAccess" && detail == "SCAN")
		index = index || opcode == "OpenIndex" || opcode == "PointLookup" || opcode == "SeekGE" || opcode == "SeekLT" || opcode == "MultiGet"
		sort = sort || opcode == "SorterSort"
		join = join || opcode == "JoinPlan"
	}
	if scan && !index {
		b.WriteString("Access: scan access is shown; a selective WHERE predicate and a matching index may reduce reads.\n")
	}
	if index {
		b.WriteString("Access: keyed or index access is shown; filters still check candidate rows.\n")
	}
	if sort {
		b.WriteString("Sorting: rows need a separate sort; an index matching ORDER BY may avoid it.\n")
	}
	if join {
		b.WriteString("Joins: estimated order is shown; actual cardinalities can change probe versus hash access.\n")
	}
	b.WriteString("For actual reads, timings and spills, run EXPLAIN ANALYZE SELECT ...; it executes the query.\n")
	b.WriteString("After substantial data changes, ANALYZE refreshes statistics used for plan choices.\n")
	return b.String()
}
