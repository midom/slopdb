package replication

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	"slopdb/db"
)

// Positions identify committed prefixes, independently of the durable frontier.
// Signing lets a member validate old tokens even after log compaction, without
// accepting a client's invented index/term as evidence of committed history.
func (n *Node) publishPosition(index, term uint64) {
	if index == 0 || term == 0 || n.config.LogsOnly {
		n.position.Store(nil)
		return
	}
	body := "sdb1:" + n.cluster + ":" + strconv.FormatUint(index, 10) + ":" + strconv.FormatUint(term, 10)
	h := hmac.New(sha256.New, []byte(n.config.Token))
	_, _ = h.Write([]byte(body))
	position := body + ":" + hex.EncodeToString(h.Sum(nil))
	n.position.Store(&position)
}

func (n *Node) currentPosition() string {
	if p := n.position.Load(); p != nil {
		return *p
	}
	return ""
}

func (n *Node) parsePosition(position string) (DurabilityToken, error) {
	if len(position) > 256 {
		return DurabilityToken{}, db.ErrInvalidPosition
	}
	parts := strings.Split(position, ":")
	if len(parts) != 5 || parts[0] != "sdb1" || parts[1] != n.cluster {
		return DurabilityToken{}, db.ErrInvalidPosition
	}
	index, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil || index == 0 || strconv.FormatUint(index, 10) != parts[2] {
		return DurabilityToken{}, db.ErrInvalidPosition
	}
	term, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil || term == 0 || strconv.FormatUint(term, 10) != parts[3] {
		return DurabilityToken{}, db.ErrInvalidPosition
	}
	mac, err := hex.DecodeString(parts[4])
	h := hmac.New(sha256.New, []byte(n.config.Token))
	_, _ = h.Write([]byte(strings.Join(parts[:4], ":")))
	if err != nil || !hmac.Equal(mac, h.Sum(nil)) {
		return DurabilityToken{}, db.ErrInvalidPosition
	}
	return DurabilityToken{Cluster: n.cluster, Index: index, Term: term}, nil
}

// AckedPosition returns a quorum-confirmed committed/applied prefix on the ready
// leader. It may cover transactions whose client replies are still in flight.
func (n *Node) AckedPosition(ctx context.Context) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := n.checkSQL(ctx); err != nil {
		return "", err
	}
	return n.currentPosition(), nil
}

// WaitPosition waits for actual local native application of an acknowledged
// prefix. It does not require a new quorum or promise fsync/durability. SQL data
// reads still obey leader/bounded-staleness admission. Without a caller deadline
// the node's OperationTimeout bounds the wait.
func (n *Node) WaitPosition(ctx context.Context, position string) error {
	if n.config.LogsOnly {
		return ErrLogsOnly
	}
	if ctx == nil {
		ctx = context.Background()
	}
	token, err := n.parsePosition(position)
	if err != nil {
		return err
	}
	if reached, err := n.positionReached(token); reached || err != nil {
		return err
	}
	if _, deadline := ctx.Deadline(); !deadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, n.config.OperationTimeout)
		defer cancel()
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if reached, err := n.positionReached(token); reached || err != nil {
			return err
		}
		notice := n.applyNotice.Load()
		if notice == nil {
			notice = &applyNotice{done: make(chan struct{})}
			if !n.applyNotice.CompareAndSwap(nil, notice) {
				continue
			}
		}
		if reached, err := n.positionReached(token); reached || err != nil {
			return err
		}
		select {
		case <-notice.done:
		case <-ctx.Done():
			return ctx.Err()
		case <-n.ctx.Done():
			return ErrClosed
		case <-ticker.C:
		}
	}
}

func (n *Node) positionReached(token DurabilityToken) (bool, error) {
	if n.closing.Load() {
		return false, ErrClosed
	}
	if err := n.failure(); err != nil {
		return false, err
	}
	if n.recovering.Load() || n.raftNode() == nil {
		return false, ErrRecovering
	}
	// A signed token was issued only for committed history. Raft preserves that
	// prefix within this incarnation, including across snapshot installation.
	return n.applied.Load() >= token.Index, nil
}
