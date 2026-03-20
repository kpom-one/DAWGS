package sonic

import (
	"fmt"
	"strings"

	cypher "github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
)

// evalNodeContext bundles the parameters needed by the non-binding evaluation functions.
type evalNodeContext struct {
	db      *Database
	id      graph.ID
	kinds   graph.Kinds
	props   *graph.Properties
	startID graph.ID
	endID   graph.ID
	rel     *graph.Relationship
	relStart *graph.Node
	relEnd   *graph.Node
}

// evalNodeCriteria evaluates a filter criteria against a node.
func evalNodeCriteria(db *Database, n *graph.Node, criteria graph.Criteria) bool {
	ctx := &evalNodeContext{db: db, id: n.ID, kinds: n.Kinds, props: n.Properties}
	return ctx.evalCriteria(criteria)
}

// evalRelCriteria evaluates a filter criteria against a relationship.
func evalRelCriteria(db *Database, r *graph.Relationship, criteria graph.Criteria) bool {
	ctx := &evalNodeContext{
		db:       db,
		id:       r.ID,
		props:    r.Properties,
		startID:  r.StartID,
		endID:    r.EndID,
		rel:      r,
		relStart: db.nodes[r.StartID],
		relEnd:   db.nodes[r.EndID],
	}
	return ctx.evalCriteria(criteria)
}

func (ctx *evalNodeContext) evalCriteria(criteria graph.Criteria) bool {
	switch c := criteria.(type) {
	case *cypher.Conjunction:
		for _, expr := range c.Expressions {
			if !ctx.evalCriteria(expr) {
				return false
			}
		}
		return true

	case *cypher.Disjunction:
		for _, expr := range c.Expressions {
			if ctx.evalCriteria(expr) {
				return true
			}
		}
		return false

	case *cypher.Negation:
		return !ctx.evalCriteria(c.Expression)

	case *cypher.Parenthetical:
		return ctx.evalCriteria(c.Expression)

	case *cypher.Comparison:
		return ctx.evalComparison(c)

	case *cypher.KindMatcher:
		return ctx.evalKindMatcher(c)

	default:
		return true
	}
}

func (ctx *evalNodeContext) evalComparison(cmp *cypher.Comparison) bool {
	left := ctx.resolveValue(cmp.Left)

	if cmp.Partials == nil || len(cmp.Partials) == 0 {
		return false
	}

	partial := cmp.Partials[0]
	right := ctx.resolveValue(partial.Right)

	switch partial.Operator {
	case cypher.OperatorEquals:
		return compareEquals(left, right)
	case cypher.OperatorNotEquals:
		return !compareEquals(left, right)
	case cypher.OperatorIn:
		return compareIn(left, right)
	case cypher.OperatorContains:
		return compareContains(left, right)
	case cypher.OperatorStartsWith:
		return compareStartsWith(left, right)
	case cypher.OperatorEndsWith:
		return compareEndsWith(left, right)
	case cypher.OperatorGreaterThan:
		return compareOrdered(left, right) > 0
	case cypher.OperatorGreaterThanOrEqualTo:
		return compareOrdered(left, right) >= 0
	case cypher.OperatorLessThan:
		return compareOrdered(left, right) < 0
	case cypher.OperatorLessThanOrEqualTo:
		return compareOrdered(left, right) <= 0
	case cypher.OperatorIs:
		return left == nil
	case cypher.OperatorIsNot:
		return left != nil
	default:
		return false
	}
}

func (ctx *evalNodeContext) evalKindMatcher(km *cypher.KindMatcher) bool {
	// Determine which kinds to match against based on the reference variable
	var targetKinds graph.Kinds

	if v, ok := km.Reference.(*cypher.Variable); ok {
		switch v.Symbol {
		case query.EdgeStartSymbol:
			if ctx.relStart != nil {
				targetKinds = ctx.relStart.Kinds
			}
		case query.EdgeEndSymbol:
			if ctx.relEnd != nil {
				targetKinds = ctx.relEnd.Kinds
			}
		default:
			targetKinds = ctx.kinds
		}
	} else {
		targetKinds = ctx.kinds
	}

	// For relationship kind matching
	if ctx.rel != nil {
		if v, ok := km.Reference.(*cypher.Variable); ok && v.Symbol == query.EdgeSymbol {
			for _, k := range km.Kinds {
				if ctx.rel.Kind == k {
					return true
				}
			}
			return false
		}
	}

	// Check if the target has all the requested kinds
	for _, k := range km.Kinds {
		found := false
		for _, tk := range targetKinds {
			if tk == k {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (ctx *evalNodeContext) resolveValue(expr any) any {
	switch e := expr.(type) {
	case *cypher.Variable:
		return nil

	case *cypher.FunctionInvocation:
		if e.Name == "id" && len(e.Arguments) > 0 {
			if v, ok := e.Arguments[0].(*cypher.Variable); ok {
				switch v.Symbol {
				case query.NodeSymbol:
					return ctx.id
				case query.EdgeSymbol:
					return ctx.id
				case query.EdgeStartSymbol:
					return ctx.startID
				case query.EdgeEndSymbol:
					return ctx.endID
				}
			}
		}
		if e.Name == "toLower" && len(e.Arguments) > 0 {
			inner := ctx.resolveValue(e.Arguments[0])
			if s, ok := inner.(string); ok {
				return strings.ToLower(s)
			}
		}
		return nil

	case *cypher.PropertyLookup:
		return ctx.resolveProperty(e)

	case *cypher.Parameter:
		return e.Value

	case *cypher.Literal:
		if e.Null {
			return nil
		}
		return e.Value

	default:
		return expr
	}
}

func (ctx *evalNodeContext) resolveProperty(lookup *cypher.PropertyLookup) any {
	var targetProps *graph.Properties

	if v, ok := lookup.Atom.(*cypher.Variable); ok {
		switch v.Symbol {
		case query.NodeSymbol:
			targetProps = ctx.props
		case query.EdgeSymbol:
			targetProps = ctx.props
		case query.EdgeStartSymbol:
			if ctx.relStart != nil {
				targetProps = ctx.relStart.Properties
			}
		case query.EdgeEndSymbol:
			if ctx.relEnd != nil {
				targetProps = ctx.relEnd.Properties
			}
		}
	}

	if targetProps == nil {
		return nil
	}

	return targetProps.Get(lookup.Symbol).Any()
}

// --- Comparison helpers ---

func compareEquals(left, right any) bool {
	if left == nil && right == nil {
		return true
	}
	if left == nil || right == nil {
		return false
	}

	// Handle graph.ID comparisons
	leftID, leftIsID := toID(left)
	rightID, rightIsID := toID(right)
	if leftIsID && rightIsID {
		return leftID == rightID
	}

	return fmt.Sprint(left) == fmt.Sprint(right)
}

func compareIn(left, right any) bool {
	switch r := right.(type) {
	case []graph.ID:
		if lid, ok := toID(left); ok {
			for _, id := range r {
				if lid == id {
					return true
				}
			}
		}
		return false
	case []int64:
		if lid, ok := toInt64(left); ok {
			for _, v := range r {
				if lid == v {
					return true
				}
			}
		}
		return false
	case []string:
		ls := fmt.Sprint(left)
		for _, v := range r {
			if ls == v {
				return true
			}
		}
		return false
	case []any:
		for _, v := range r {
			if compareEquals(left, v) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func compareContains(left, right any) bool {
	ls, lok := left.(string)
	rs, rok := right.(string)
	if lok && rok {
		return strings.Contains(ls, rs)
	}
	return false
}

func compareStartsWith(left, right any) bool {
	ls, lok := left.(string)
	rs, rok := right.(string)
	if lok && rok {
		return strings.HasPrefix(ls, rs)
	}
	return false
}

func compareEndsWith(left, right any) bool {
	ls, lok := left.(string)
	rs, rok := right.(string)
	if lok && rok {
		return strings.HasSuffix(ls, rs)
	}
	return false
}

func compareOrdered(left, right any) int {
	lf, lok := toFloat64(left)
	rf, rok := toFloat64(right)
	if lok && rok {
		if lf < rf {
			return -1
		}
		if lf > rf {
			return 1
		}
		return 0
	}
	return 0
}

// --- Binding-aware expression evaluation ---

// evalBindingExpr evaluates a Cypher expression against a binding row.
// Returns true/false for boolean expressions (WHERE clauses).
func evalBindingExpr(db *Database, b binding, expr cypher.Expression) bool {
	switch e := expr.(type) {
	case *cypher.Conjunction:
		for _, sub := range e.Expressions {
			if !evalBindingExpr(db, b, sub) {
				return false
			}
		}
		return true

	case *cypher.Disjunction:
		for _, sub := range e.Expressions {
			if evalBindingExpr(db, b, sub) {
				return true
			}
		}
		return false

	case *cypher.Negation:
		return !evalBindingExpr(db, b, e.Expression)

	case *cypher.Parenthetical:
		return evalBindingExpr(db, b, e.Expression)

	case *cypher.Comparison:
		return evalBindingComparison(db, b, e)

	case *cypher.KindMatcher:
		return evalBindingKindMatcher(b, e)

	default:
		return true
	}
}

// evalBindingComparison evaluates a comparison expression against a binding row.
func evalBindingComparison(db *Database, b binding, cmp *cypher.Comparison) bool {
	left := resolveBindingValue(db, b, cmp.Left)

	if len(cmp.Partials) == 0 {
		return false
	}

	partial := cmp.Partials[0]
	right := resolveBindingValue(db, b, partial.Right)

	switch partial.Operator {
	case cypher.OperatorEquals:
		return compareEquals(left, right)
	case cypher.OperatorNotEquals:
		return !compareEquals(left, right)
	case cypher.OperatorIn:
		return compareIn(left, right)
	case cypher.OperatorContains:
		return compareContains(left, right)
	case cypher.OperatorStartsWith:
		return compareStartsWith(left, right)
	case cypher.OperatorEndsWith:
		return compareEndsWith(left, right)
	case cypher.OperatorGreaterThan:
		return compareOrdered(left, right) > 0
	case cypher.OperatorGreaterThanOrEqualTo:
		return compareOrdered(left, right) >= 0
	case cypher.OperatorLessThan:
		return compareOrdered(left, right) < 0
	case cypher.OperatorLessThanOrEqualTo:
		return compareOrdered(left, right) <= 0
	case cypher.OperatorIs:
		return left == nil
	case cypher.OperatorIsNot:
		return left != nil
	default:
		return false
	}
}

// evalBindingKindMatcher checks if a bound entity matches the specified kinds.
func evalBindingKindMatcher(b binding, km *cypher.KindMatcher) bool {
	v, ok := km.Reference.(*cypher.Variable)
	if !ok {
		return false
	}

	entity, exists := b[v.Symbol]
	if !exists || entity == nil {
		return false
	}

	var targetKinds graph.Kinds
	switch e := entity.(type) {
	case *graph.Node:
		targetKinds = e.Kinds
	case *graph.Relationship:
		// For relationships, check if any matcher kind matches the rel kind
		for _, k := range km.Kinds {
			if e.Kind == k {
				return true
			}
		}
		return false
	default:
		return false
	}

	// For nodes: all specified kinds must be present (AND semantics)
	for _, k := range km.Kinds {
		if !targetKinds.ContainsOneOf(k) {
			return false
		}
	}
	return true
}

// resolveBindingValue resolves an expression to a concrete value using a binding row.
func resolveBindingValue(db *Database, b binding, expr any) any {
	switch e := expr.(type) {
	case *cypher.Variable:
		return b[e.Symbol]

	case *cypher.PropertyLookup:
		return resolveBindingProperty(db, b, e)

	case *cypher.FunctionInvocation:
		return resolveBindingFunction(db, b, e)

	case *cypher.Parameter:
		return e.Value

	case *cypher.Literal:
		if e.Null {
			return nil
		}
		return stripStringQuotes(e.Value)

	case *cypher.Parenthetical:
		return resolveBindingValue(db, b, e.Expression)

	default:
		return expr
	}
}

// resolveBindingProperty resolves a property lookup against a binding row.
func resolveBindingProperty(db *Database, b binding, lookup *cypher.PropertyLookup) any {
	atom := resolveBindingValue(db, b, lookup.Atom)
	if atom == nil {
		return nil
	}

	switch e := atom.(type) {
	case *graph.Node:
		if e.Properties == nil {
			return nil
		}
		return e.Properties.Get(lookup.Symbol).Any()
	case *graph.Relationship:
		if e.Properties == nil {
			return nil
		}
		return e.Properties.Get(lookup.Symbol).Any()
	default:
		return nil
	}
}

// resolveBindingFunction evaluates a function call against a binding row.
func resolveBindingFunction(db *Database, b binding, fn *cypher.FunctionInvocation) any {
	switch fn.Name {
	case "id":
		if len(fn.Arguments) == 0 {
			return nil
		}
		arg := resolveBindingValue(db, b, fn.Arguments[0])
		switch e := arg.(type) {
		case *graph.Node:
			return e.ID
		case *graph.Relationship:
			return e.ID
		}
		return nil

	case "type":
		if len(fn.Arguments) == 0 {
			return nil
		}
		arg := resolveBindingValue(db, b, fn.Arguments[0])
		if rel, ok := arg.(*graph.Relationship); ok {
			return rel.Kind.String()
		}
		return nil

	case "toLower":
		if len(fn.Arguments) == 0 {
			return nil
		}
		arg := resolveBindingValue(db, b, fn.Arguments[0])
		if s, ok := arg.(string); ok {
			return strings.ToLower(s)
		}
		return nil

	case "toUpper":
		if len(fn.Arguments) == 0 {
			return nil
		}
		arg := resolveBindingValue(db, b, fn.Arguments[0])
		if s, ok := arg.(string); ok {
			return strings.ToUpper(s)
		}
		return nil

	case "count", "collect", "sum", "avg", "min", "max":
		// Aggregation functions are not yet supported in this evaluator
		return nil

	case "labels", "keys":
		if len(fn.Arguments) == 0 {
			return nil
		}
		arg := resolveBindingValue(db, b, fn.Arguments[0])
		if fn.Name == "labels" {
			if node, ok := arg.(*graph.Node); ok {
				return node.Kinds.Strings()
			}
		}
		return nil

	default:
		return nil
	}
}

// stripStringQuotes removes surrounding single or double quotes from a string value.
func stripStringQuotes(v any) any {
	s, ok := v.(string)
	if !ok {
		return v
	}
	if len(s) >= 2 {
		if (s[0] == '\'' && s[len(s)-1] == '\'') || (s[0] == '"' && s[len(s)-1] == '"') {
			return s[1 : len(s)-1]
		}
	}
	return v
}

// --- Type coercion helpers ---

func toID(v any) (graph.ID, bool) {
	switch tv := v.(type) {
	case graph.ID:
		return tv, true
	case int64:
		return graph.ID(tv), true
	case uint64:
		return graph.ID(tv), true
	case int:
		return graph.ID(tv), true
	default:
		return 0, false
	}
}

func toInt64(v any) (int64, bool) {
	switch tv := v.(type) {
	case int64:
		return tv, true
	case int:
		return int64(tv), true
	case graph.ID:
		return int64(tv), true
	case float64:
		return int64(tv), true
	default:
		return 0, false
	}
}

func toFloat64(v any) (float64, bool) {
	switch tv := v.(type) {
	case float64:
		return tv, true
	case float32:
		return float64(tv), true
	case int:
		return float64(tv), true
	case int64:
		return float64(tv), true
	case graph.ID:
		return float64(tv), true
	default:
		return 0, false
	}
}
