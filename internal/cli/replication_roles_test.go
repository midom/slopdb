package cli

import (
	"flag"
	"testing"
)

func TestReplicationRoleFlags(t *testing.T) {
	for _, tc := range []struct {
		args                []string
		nonVoting, logsOnly bool
	}{
		{[]string{"--raft-non-voting"}, true, false},
		{[]string{"--raft-logs-only"}, false, true},
		{[]string{"--raft-non-voting", "--raft-logs-only"}, true, true},
	} {
		var f replicationFlags
		flags := flag.NewFlagSet("roles", flag.ContinueOnError)
		f.register(flags)
		if err := flags.Parse(tc.args); err != nil {
			t.Fatal(err)
		}
		if !f.enabled() {
			t.Fatalf("role flags did not enable replication: %v", tc.args)
		}
		if f.config.NonVoting != tc.nonVoting || f.config.LogsOnly != tc.logsOnly {
			t.Fatalf("role flags not recorded: %v: nonVoting=%v logsOnly=%v", tc.args, f.config.NonVoting, f.config.LogsOnly)
		}
	}
}
