package main

import (
	"fmt"
	"strconv"
	"strings"
	"text/tabwriter"

	"slopdb/db"
)

func printLockWaits() {
	cliPrint(formatLockWaits(currentDB.LockWaits()))
}

func formatLockWaits(waits []db.LockWaitInfo) string {
	if len(waits) == 0 {
		return "No active lock waits.\n"
	}
	var output strings.Builder
	w := tabwriter.NewWriter(&output, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "Session\tBlockers\tKind\tMode\tWait\tResource\tValues")
	for _, wait := range waits {
		blockers := make([]string, len(wait.BlockingSessionIDs))
		for i, id := range wait.BlockingSessionIDs {
			blockers[i] = strconv.FormatUint(id, 10)
		}
		mode := "shared"
		if wait.Exclusive {
			mode = "exclusive"
		}
		values := make([]string, len(wait.Values))
		for i, value := range wait.Values {
			values[i] = strconv.Quote(value.String())
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\t%s\t%s\n", wait.SessionID,
			strings.Join(blockers, ","), wait.ResourceKind, mode, wait.WaitDuration,
			strconv.Quote(wait.Resource), strings.Join(values, ", "))
	}
	_ = w.Flush()
	return output.String()
}
