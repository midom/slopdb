package sql

import (
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"
)

const sessionVariableRead = "__SESSION_VARIABLE"
const sessionVariableAssign = "__SET_SESSION_VARIABLE"
const systemVariableRead = "__SYSTEM_VARIABLE"

// SessionState stores typed connection-local values. The owning session gate
// serializes access; values are copied at the boundary, never catalog/WAL data.
// Transactions/savepoints deliberately do not capture this state (MySQL rules).
type SessionState struct {
	variables map[string]Value
	bytes     int64
}

func (s *SessionState) Variable(name string) (Value, bool) {
	v, ok := s.variables[name]
	return ownedSessionValue(v), ok
}
func (s *SessionState) SetVariable(name string, v Value, budget int64) error {
	charge := sessionValueBytes(name, v)
	old := int64(0)
	if before, ok := s.variables[name]; ok {
		old = sessionValueBytes(name, before)
	}
	if charge > budget || s.bytes-old > budget-charge {
		return fmt.Errorf("session variables exceed memory budget")
	}
	if s.variables == nil {
		s.variables = make(map[string]Value)
	}
	s.variables[name] = ownedSessionValue(v)
	s.bytes = s.bytes - old + charge
	return nil
}
func (s *SessionState) Reset() { s.variables = nil; s.bytes = 0 }
func ownedSessionValue(v Value) Value {
	if v.Blob != nil {
		v.Blob = append([]byte{}, v.Blob...)
	}
	return v
}
func sessionValueBytes(name string, v Value) int64 {
	return 256 + int64(len(name)) + int64(len(v.Text)) + int64(len(v.Blob)) + int64(len(v.NumericText)) + int64(len(v.NumericExact))
}

// SessionVariableStmt changes only private session state. Expressions are still
// ordinary SQL ASTs, including scalar subqueries and prepared bindings.
type SessionVariableStmt struct{ Assignments []Expression }

func (*SessionVariableStmt) stmt() {}
func (s *SessionVariableStmt) ReadsData() bool {
	found := false
	for _, e := range s.Assignments {
		_ = walkExpression(e, func(e Expression) (bool, error) {
			switch e.(type) {
			case *SubqueryExpr, *ExistsExpr:
				found = true
			}
			return found, nil
		})
	}
	return found
}
func (s *Schema) ResetSessionState() { s.sessionState.Reset() }
func (s *Schema) ExecuteSessionVariables(stmt *SessionVariableStmt) error {
	if !s.MySQLMode() {
		return fmt.Errorf("user variables require MySQL dialect")
	}
	for _, e := range stmt.Assignments {
		if err := s.checkInterrupt(); err != nil {
			return err
		}
		if _, err := s.evalExpr(e, nil); err != nil {
			return err
		}
	}
	return nil
}
func sessionVariableFunctions() map[string]FunctionSpec {
	return map[string]FunctionSpec{
		systemVariableRead: {Min: 2, Max: 2, Return: ColUnknown, Dialects: OnlyMySQL, Volatile: true, Metadata: func([]ColumnDef) ColumnDef { return ColumnDef{ColType: ColUnknown} }, Eval: func(s *Schema, a []Value) (Value, error) {
			name := strings.ToLower(a[0].Text)
			if name == "transaction_isolation" || name == "tx_isolation" {
				level := s.IsolationLevel()
				if a[1].Int != 0 {
					level = RepeatableRead
				}
				return NewTextValue(strings.ReplaceAll(string(level), " ", "-")), nil
			}
			options, ok := SessionContextFrom(s.ctx)
			if !ok || options.SystemVariable == nil {
				return NewNullValue(), fmt.Errorf("system variables require a server connection")
			}
			if err := s.checkInterrupt(); err != nil {
				return NewNullValue(), err
			}
			v, err := options.SystemVariable(s.ctx, a[0].Text, a[1].Int != 0)
			return ownedSessionValue(v), err
		}},
		sessionVariableRead: {Min: 1, Max: 1, Return: ColUnknown, Dialects: OnlyMySQL, Volatile: true, Metadata: func([]ColumnDef) ColumnDef { return ColumnDef{ColType: ColUnknown} }, Eval: func(s *Schema, a []Value) (Value, error) {
			if a[0].Null {
				return NewNullValue(), fmt.Errorf("session variable name is NULL")
			}
			v, found := s.sessionState.Variable(strings.ToLower(a[0].Text))
			if !found {
				return NewNullValue(), nil
			}
			return v, nil
		}},
		sessionVariableAssign: {Min: 2, Max: 2, Return: ColUnknown, Dialects: OnlyMySQL, Volatile: true, Metadata: func(a []ColumnDef) ColumnDef {
			if len(a) == 2 {
				return a[1]
			}
			return ColumnDef{ColType: ColUnknown}
		}, Eval: func(s *Schema, a []Value) (Value, error) {
			if a[0].Null {
				return NewNullValue(), fmt.Errorf("session variable name is NULL")
			}
			if err := s.checkInterrupt(); err != nil {
				return NewNullValue(), err
			}
			if err := s.sessionState.SetVariable(strings.ToLower(a[0].Text), a[1], s.memoryBudget()); err != nil {
				return NewNullValue(), err
			}
			return ownedSessionValue(a[1]), nil
		}},
	}
}
func (f *FuncCallExpr) ResultName() string {
	if (f.Name == sessionVariableRead || f.Name == sessionVariableAssign || f.Name == systemVariableRead) && len(f.Args) > 0 {
		if name, ok := f.Args[0].(*LiteralExpr); ok {
			prefix := "@"
			if f.Name == systemVariableRead {
				prefix = "@@"
			}
			return prefix + name.Value.Text
		}
	}
	return f.Name
}
func sessionVariableName(e Expression) (string, bool) {
	f, ok := e.(*FuncCallExpr)
	if !ok || f.Name != sessionVariableRead || len(f.Args) != 1 {
		return "", false
	}
	name, ok := f.Args[0].(*LiteralExpr)
	if !ok {
		return "", false
	}
	return name.Value.Text, true
}
func variableExpression(name string, value Expression) Expression {
	args := []Expression{&LiteralExpr{Value: NewTextValue(name)}}
	fn := sessionVariableRead
	if value != nil {
		fn = sessionVariableAssign
		args = append(args, value)
	}
	return &FuncCallExpr{Name: fn, Args: args}
}
func (p *Parser) parseSessionVariable() (Expression, error) {
	p.advance()
	if p.cur.Value == "@" && p.cur.Type == TokInvalid {
		p.advance()
		global := false
		if p.tok.Peek().Type == TokDot {
			scope := strings.ToLower(p.cur.Value)
			switch scope {
			case "global":
				global = true
			case "session", "local":
			default:
				return nil, fmt.Errorf("unknown system variable scope %s", scope)
			}
			p.advance()
			p.advance()
		}
		if p.cur.Type != TokIdent && p.cur.Type != TokKeyword {
			return nil, fmt.Errorf("expected system variable name")
		}
		name := strings.ToLower(p.cur.Value)
		p.advance()
		scope := int64(0)
		if global {
			scope = 1
		}
		return &FuncCallExpr{Name: systemVariableRead, Args: []Expression{&LiteralExpr{Value: NewTextValue(name)}, &LiteralExpr{Value: NewIntValue(scope)}}}, nil
	}
	switch p.cur.Type {
	case TokIdent, TokKeyword, TokString, TokInt:
	default:
		return nil, fmt.Errorf("expected user variable name after @")
	}
	name := p.cur.Value
	quoted := p.cur.Type == TokString
	p.advance()
	if !quoted {
		for p.cur.Type == TokDot {
			p.advance()
			if p.cur.Type != TokIdent && p.cur.Type != TokKeyword && p.cur.Type != TokInt {
				return nil, fmt.Errorf("expected user variable name component")
			}
			name += "." + p.cur.Value
			p.advance()
		}
	}
	if name == "" || utf8.RuneCountInString(name) > 64 {
		return nil, fmt.Errorf("user variable name must contain 1 to 64 characters")
	}
	return variableExpression(strings.ToLower(name), nil), nil
}
func (p *Parser) parseSessionVariableSetting() (Statement, error) {
	p.advance()
	stmt := &SessionVariableStmt{}
	for {
		if p.cur.Type != TokInvalid || p.cur.Value != "@" {
			return nil, fmt.Errorf("expected user variable assignment")
		}
		left, e := p.parseSessionVariable()
		if e != nil {
			return nil, e
		}
		name, ok := sessionVariableName(left)
		if !ok {
			return nil, fmt.Errorf("system variable assignments require server setting syntax")
		}
		if p.cur.Type != TokEq && p.cur.Type != TokAssign {
			return nil, fmt.Errorf("expected = or := after user variable")
		}
		p.advance()
		value, e := p.parseExpression()
		if e != nil {
			return nil, e
		}
		stmt.Assignments = append(stmt.Assignments, variableExpression(name, value))
		if p.cur.Type != TokComma {
			break
		}
		p.advance()
	}
	return stmt, nil
}

// Session variables cannot be serialized into persistent schema definitions.
func expressionUsesSessionVariable(expr Expression) bool {
	found := false
	_ = walkExpression(expr, func(e Expression) (bool, error) {
		if f, ok := e.(*FuncCallExpr); ok && (f.Name == sessionVariableRead || f.Name == sessionVariableAssign || f.Name == systemVariableRead) {
			found = true
		}
		return found, nil
	})
	return found
}

// LocalOnly permits follower inspection without treating a session assignment
// as a replicated write. Any table/column/subquery dependency keeps normal read
// admission; lock/catalog/full-text operations are conservatively excluded.
func (s *SessionVariableStmt) LocalOnly() bool {
	for _, e := range s.Assignments {
		if !sessionExpressionLocal(e) {
			return false
		}
	}
	return true
}
func sessionExpressionLocal(expr Expression) bool {
	local := true
	_ = walkExpression(expr, func(e Expression) (bool, error) {
		switch e := e.(type) {
		case *ColumnRefExpr:
			local = false
			return true, nil
		case *SubqueryExpr:
			local = local && sessionSelectLocal(e.Stmt)
			return true, nil
		case *ExistsExpr:
			local = local && sessionSelectLocal(e.Stmt)
			return true, nil
		case *FuncCallExpr:
			if e.Window != nil {
				local = false
				return true, nil
			}
			switch strings.ToUpper(e.Name) {
			case sessionVariableRead, sessionVariableAssign, systemVariableRead, "RAND", "BENCHMARK", "NOW", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "LOCALTIME", "LOCALTIMESTAMP", "CURRENT_USER", "DATABASE", "VERSION", "CONNECTION_ID", "LAST_INSERT_ID", "ROW_COUNT", "FOUND_ROWS":
			default:
				if e.Name == fulltextFunction || !FunctionDeterministic(DialectMySQL, e.Name) {
					local = false
					return true, nil
				}
			}
		}
		return false, nil
	})
	return local
}
func sessionSelectLocal(q *SelectStmt) bool {
	if q == nil || q.From != "" || q.FromExpr != nil || len(q.Joins) > 0 || q.With != nil || q.Virtual != nil || q.Compound != nil || q.Lock != nil || q.SelectAll || len(q.Windows) > 0 {
		return false
	}
	for _, c := range q.Columns {
		if c.Star || !sessionExpressionLocal(c.Expr) {
			return false
		}
	}
	if !sessionExpressionLocal(q.Where) || !sessionExpressionLocal(q.Having) {
		return false
	}
	for _, e := range q.GroupBy {
		if !sessionExpressionLocal(e) {
			return false
		}
	}
	for _, o := range q.OrderBy {
		if !sessionExpressionLocal(o.Expr) {
			return false
		}
	}
	return true
}

func statementUsesSessionVariables(stmt Statement) bool {
	found := false
	namespaceVisit(reflect.ValueOf(stmt), func(v any) {
		if f, ok := v.(*FuncCallExpr); ok && (f.Name == sessionVariableRead || f.Name == sessionVariableAssign || f.Name == systemVariableRead) {
			found = true
		}
	})
	return found
}
