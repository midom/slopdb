package sql

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

// MySQL uses two 30-bit seeds. Constant initializers belong to the expression,
// while a column-dependent initializer resets for each evaluated row.
type mysqlRandState struct {
	seed1, seed2 uint64
	constant     bool
}

func mysqlSeededRand(seed uint32) *mysqlRandState {
	const maximum = uint64(0x3fffffff)
	return &mysqlRandState{seed1: uint64(seed*0x10001+55555555) % maximum, seed2: uint64(seed*0x10000001) % maximum}
}

func (r *mysqlRandState) next() float64 {
	const maximum = uint64(0x3fffffff)
	r.seed1 = (r.seed1*3 + r.seed2) % maximum
	r.seed2 = (r.seed1 + r.seed2 + 33) % maximum
	return float64(r.seed1) / float64(maximum)
}

func (s *Schema) evalMySQLRand(expression *FuncCallExpr, row map[string]Value) (Value, bool, error) {
	if len(expression.Args) > 1 {
		return NewNullValue(), true, fmt.Errorf("wrong number of arguments to RAND")
	}
	if s.mysqlRand == nil {
		s.mysqlRand = make(map[*FuncCallExpr]*mysqlRandState)
	}
	key := expression
	if len(expression.Args) == 0 {
		key = nil
	}
	state := s.mysqlRand[key]
	if state != nil && (key == nil || state.constant) {
		return NewRealValue(state.next()), true, nil
	}
	if key == nil {
		var seed [8]byte
		if _, err := rand.Read(seed[:]); err != nil {
			return NewNullValue(), true, err
		}
		state = mysqlSeededRand(binary.LittleEndian.Uint32(seed[:4]))
		state.seed2 = uint64(binary.LittleEndian.Uint32(seed[4:])) % 0x3fffffff
	} else {
		value, err := s.evalExpr(expression.Args[0], row)
		if err != nil {
			return NewNullValue(), true, err
		}
		state = mysqlSeededRand(mysqlRandSeed(value))
		state.constant = true
		_ = walkExpression(expression.Args[0], func(e Expression) (bool, error) {
			switch x := e.(type) {
			case *ColumnRefExpr, *SubqueryExpr, *ExistsExpr:
				state.constant = false
				return true, nil
			case *FuncCallExpr:
				if !deterministicFunction(x.Name) {
					state.constant = false
					return true, nil
				}
			}
			return false, nil
		})
	}
	s.mysqlRand[key] = state
	return NewRealValue(state.next()), true, nil
}

func mysqlRandSeed(value Value) uint32 {
	if value.HexLiteral {
		return uint32(mysqlHexNumber(value).Uint)
	}
	if value.Null {
		return 0
	}
	if value.Type == ColUnsigned {
		return uint32(value.Uint)
	}
	if value.Type == ColText || value.Type == ColBlob {
		text := strings.TrimSpace(mysqlStringValue(value))
		if strings.HasPrefix(text, "+") {
			text = text[1:]
		}
		if !strings.HasPrefix(text, "-") {
			end := 0
			for end < len(text) && text[end] >= '0' && text[end] <= '9' {
				end++
			}
			if end == 0 {
				return 0
			}
			n, _ := strconv.ParseUint(text[:end], 10, 64)
			return uint32(n)
		}
	}
	return uint32(mysqlFunctionInt(value))
}
