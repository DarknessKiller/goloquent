package goloquent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"cloud.google.com/go/datastore"
	"github.com/RevenueMonster/goloquent/expr"
)

type operator int

// JSON :
const (
	Equal operator = iota
	EqualTo
	NotEqual
	LessThan
	LessEqual
	GreaterThan
	GreaterEqual
	AnyLike
	Like
	NotLike
	ContainAny
	ContainAll
	In
	NotIn
	IsObject
	IsArray
	IsType
	MatchAgainst
)

type sortDirection int

const (
	ascending sortDirection = iota
	descending
)

type order struct {
	field     string
	direction sortDirection
}

type locked int

// lock mode
const (
	ReadLock locked = iota + 1
	WriteLock
)

const (
	maxLimit             = 10000
	keyFieldName         = "__key__"
	contextResolutionKey = "_goloquent_context_query"
)

func checkSinglePtr(it interface{}) error {
	v := reflect.ValueOf(it)
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("goloquent: entity must be addressable")
	}
	v = v.Elem()
	if v.Kind() != reflect.Struct || isBaseType(v.Type()) {
		return fmt.Errorf("goloquent: entity data type must be struct")
	}
	return nil
}

type group struct {
	isGroup bool
	data    []interface{}
}

type scope struct {
	table           string
	distinctOn      []string
	projection      []string
	omits           []string
	ancestors       []group
	filters         []Filter
	orders          []interface{}
	limit           int32
	offset          int32
	errs            []error
	noScope         bool
	noResolution    bool
	lockMode        locked
	replicaResolver replicaResolver
}

func (s scope) append(s2 scope) scope {
	s.projection = append(s.projection, s2.projection...)
	s.filters = append(s.filters, s2.filters...)
	s.orders = append(s.orders, s2.orders...)
	return s
}

// Query :
type Query struct {
	db *DB
	scope
}

func newQuery(db *DB) *Query {
	return &Query{
		db: db.clone(),
		scope: scope{
			limit:  -1,
			offset: -1,
		},
	}
}

func (q *Query) clone() *Query {
	ss := q.scope
	return &Query{
		db:    q.db.clone(),
		scope: ss,
	}
}

func (q *Query) append(query *Query) *Query {
	q.scope = q.scope.append(query.scope)
	return q
}

func (q *Query) getError() error {
	if len(q.errs) > 0 {
		buf := new(bytes.Buffer)
		for _, err := range q.errs {
			buf.WriteString(fmt.Sprintf("%v", err))
		}
		return fmt.Errorf("%s", buf.String())
	}
	return nil
}

// Select :
func (q *Query) Select(fields ...string) *Query {
	q = q.clone()
	arr := make([]string, 0, len(fields))
	for _, f := range fields {
		f := strings.TrimSpace(f)
		if f == "" {
			q.errs = append(q.errs, fmt.Errorf("goloquent: invalid `Select` value %q", f))
			return q
		}
		arr = append(arr, f)
	}
	q.projection = append(q.projection, arr...)
	return q
}

// DistinctOn :
func (q *Query) DistinctOn(fields ...string) *Query {
	q = q.clone()
	arr := make([]string, 0, len(fields))
	for _, f := range fields {
		f := strings.TrimSpace(f)
		if f == "" || f == "*" {
			q.errs = append(q.errs, fmt.Errorf("goloquent: invalid `DistinctOn` value %q", f))
			return q
		}
		arr = append(arr, f)
	}
	q.distinctOn = append(q.distinctOn, arr...)
	return q
}

// Omit :
func (q *Query) Omit(fields ...string) *Query {
	q = q.clone()
	arr := make([]string, 0, len(fields))
	for _, f := range fields {
		f := strings.TrimSpace(f)
		if f == "" || f == "*" {
			q.errs = append(q.errs, fmt.Errorf("goloquent: invalid omit value %v", f))
			return q
		}
		arr = append(arr, f)
	}
	// Primary key cannot be omited
	dict := newDictionary(append(q.projection, arr...))
	dict.delete(keyFieldName)
	dict.delete(pkColumn)
	q.omits = dict.keys()
	return q
}

// Unscoped :
func (q *Query) Unscoped() *Query {
	q.noScope = true
	return q
}

// Unscoped :
func (q *Query) ReplicaResolver(resolver replicaResolver) *Query {
	q.replicaResolver = resolver
	return q
}

// IgnoreResolution :
func (q *Query) IgnoreResolution() *Query {
	q.noResolution = true
	return q
}

// Find :
func (q *Query) Find(ctx context.Context, key *datastore.Key, model interface{}) error {
	if err := q.getError(); err != nil {
		return err
	}
	if err := checkSinglePtr(model); err != nil {
		return err
	}
	if key == nil || key.Incomplete() {
		return fmt.Errorf("goloquent: find action with invalid key value, %q", key)
	}
	q = q.Where(keyFieldName, "=", key).Limit(1)
	return newBuilder(q, operationRead).get(ctx, model, true)
}

// First :
func (q *Query) First(ctx context.Context, model interface{}) error {
	q = q.clone()
	if err := q.getError(); err != nil {
		return err
	}
	if err := checkSinglePtr(model); err != nil {
		return err
	}
	q.Limit(1)
	return newBuilder(q, operationRead).get(ctx, model, false)
}

// Get :
func (q *Query) Get(ctx context.Context, model interface{}) error {
	q = q.clone()
	if err := q.getError(); err != nil {
		return err
	}
	return newBuilder(q, operationRead).getMulti(ctx, model)
}

// Paginate :
func (q *Query) Paginate(ctx context.Context, p *Pagination, model interface{}) error {
	pkSortExist := false
	if err := q.getError(); err != nil {
		return err
	}
	q = q.clone()
	if p.query != nil {
		q = q.append(p.query)
	}
	if p.Limit > maxLimit {
		return fmt.Errorf("goloquent: limit overflow : %d, maximum limit : %d", p.Limit, maxLimit)
	} else if p.Limit <= 0 {
		p.Limit = defaultLimit
	}
	q = q.Limit(int(p.Limit) + 1)
	if len(q.orders) > 0 {
		for _, o := range q.orders {
			if x, isOk := o.(expr.Sort); isOk && x.Name == pkColumn {
				pkSortExist = true
				break
			}
		}
		if !pkSortExist {
			lastField := q.orders[len(q.orders)-1].(expr.Sort)
			k := pkColumn
			if lastField.Direction == expr.Descending {
				k = "-" + k
			}
			q = q.OrderBy(k)
		}
	} else {
		q = q.OrderBy(pkColumn)
	}
	return newBuilder(q, operationRead).paginate(ctx, p, model)
}

// Ancestor :
func (q *Query) Ancestor(ancestor *datastore.Key) *Query {
	if ancestor == nil {
		q.errs = append(q.errs, errors.New("goloquent: ancestor key cannot be nil"))
		return q
	}
	if ancestor.Incomplete() {
		q.errs = append(q.errs, fmt.Errorf("goloquent: ancestor key is incomplete, %v", ancestor))
		return q
	}
	q = q.clone()
	q.ancestors = append(q.ancestors, group{false, []interface{}{ancestor}})
	return q
}

// AnyOfAncestor :
func (q *Query) AnyOfAncestor(ancestors ...*datastore.Key) *Query {
	if len(ancestors) <= 0 {
		q.errs = append(q.errs, errors.New(`goloquent: "AnyOfAncestor" cannot be empty`))
		return q
	}
	g := group{true, make([]interface{}, 0)}
	for _, a := range ancestors {
		if a == nil {
			q.errs = append(q.errs, errors.New("goloquent: ancestor key cannot be nil"))
			return q
		}
		if a.Incomplete() {
			q.errs = append(q.errs, fmt.Errorf("goloquent: ancestor key is incomplete, %v", a))
			return q
		}
		g.data = append(g.data, a)
	}
	q = q.clone()
	q.ancestors = append(q.ancestors, g)
	return q
}

func (q *Query) where(field, op string, value interface{}, isJSON bool) *Query {
	op = strings.TrimSpace(strings.ToLower(op))
	var optr operator

	switch op {
	case "=", "eq", "$eq", "equal":
		optr = Equal
	case "!=", "<>", "ne", "$ne", "notequal", "not equal":
		optr = NotEqual
	case ">", "!<", "gt", "$gt":
		optr = GreaterThan
	case "<", "!>", "lt", "$lt":
		optr = LessThan
	case ">=", "gte", "$gte":
		optr = GreaterEqual
	case "<=", "lte", "$lte":
		optr = LessEqual
	case "in", "$in":
		optr = In
	case "nin", "!in", "$nin", "not in", "notin":
		optr = NotIn
	case "anylike":
		optr = AnyLike
	case "like", "$like":
		if isJSON {
			q.errs = append(q.errs, fmt.Errorf("goloquent: invalid operator %q for json", op))
			return q
		}
		optr = Like
	case "nlike", "!like", "$nlike":
		if isJSON {
			q.errs = append(q.errs, fmt.Errorf("goloquent: invalid operator %q for json", op))
			return q
		}
		optr = NotLike
	case "match":
		optr = MatchAgainst
	default:
		if !isJSON {
			q.errs = append(q.errs, fmt.Errorf("goloquent: invalid operator %q", op))
			return q
		}

		switch op {
		case "containany":
			optr = ContainAny
		case "istype":
			optr = IsType
		case "isobject":
			optr = IsObject
		case "isarray":
			optr = IsArray
		default:
			q.errs = append(q.errs, fmt.Errorf("goloquent: invalid operator %q for json", op))
			return q
		}
	}

	q.filters = append(q.filters, Filter{
		field:    field,
		operator: optr,
		value:    value,
		isJSON:   isJSON,
	})
	return q
}

// Where :
func (q *Query) Where(field string, op string, value interface{}) *Query {
	q = q.clone()
	return q.where(field, op, value, false)
}

// WhereEqual :
func (q *Query) WhereEqual(field string, v interface{}) *Query {
	return q.Where(field, "=", v)
}

// WhereNotEqual :
func (q *Query) WhereNotEqual(field string, v interface{}) *Query {
	return q.Where(field, "!=", v)
}

// WhereNull :
func (q *Query) WhereNull(field string) *Query {
	return q.Where(field, "=", nil)
}

// WhereNotNull :
func (q *Query) WhereNotNull(field string) *Query {
	return q.Where(field, "<>", nil)
}

// WhereIn :
func (q *Query) WhereIn(field string, v interface{}) *Query {
	vv := reflect.Indirect(reflect.ValueOf(v))
	t := vv.Type()
	if !vv.IsValid() || (t.Kind() != reflect.Slice && t.Kind() != reflect.Array) {
		q.errs = append(q.errs, fmt.Errorf(`goloquent: value must be either slice or array for "WhereIn"`))
		return q
	}
	return q.Where(field, "in", v)
}

// WhereNotIn :
func (q *Query) WhereNotIn(field string, v interface{}) *Query {
	vv := reflect.Indirect(reflect.ValueOf(v))
	t := vv.Type()
	if !vv.IsValid() || (t.Kind() != reflect.Slice && t.Kind() != reflect.Array) {
		q.errs = append(q.errs, fmt.Errorf(`goloquent: value must be either slice or array for "WhereNotIn"`))
		return q
	}
	return q.Where(field, "nin", v)
}

// WhereLike :
func (q *Query) WhereLike(field, v string) *Query {
	return q.Where(field, "like", v)
}

// WhereNotLike :
func (q *Query) WhereNotLike(field, v string) *Query {
	return q.Where(field, "nlike", v)
}

// WhereAnyLike :
func (q *Query) WhereAnyLike(field string, v interface{}) *Query {
	vv := reflect.Indirect(reflect.ValueOf(v))
	t := vv.Type()
	if !vv.IsValid() || (t.Kind() != reflect.Slice && t.Kind() != reflect.Array) {
		q.errs = append(q.errs, fmt.Errorf(`goloquent: value must be either slice or array for "WhereAnyLike"`))
		return q
	}
	return q.Where(field, "anylike", v)
}

// WhereJSON :
func (q *Query) WhereJSON(field, op string, v interface{}) *Query {
	return q.where(field, op, v, true)
}

// WhereJSONEqual :
func (q *Query) WhereJSONEqual(field string, v interface{}) *Query {
	return q.WhereJSON(field, "=", v)
}

// WhereJSONNotEqual :
func (q *Query) WhereJSONNotEqual(field string, v interface{}) *Query {
	return q.WhereJSON(field, "!=", v)
}

// WhereJSONIn :
func (q *Query) WhereJSONIn(field string, v []interface{}) *Query {
	return q.WhereJSON(field, "in", v)
}

// WhereJSONNotIn :
func (q *Query) WhereJSONNotIn(field string, v []interface{}) *Query {
	return q.WhereJSON(field, "nin", v)
}

// WhereJSONContainAny :
func (q *Query) WhereJSONContainAny(field string, v interface{}) *Query {
	return q.WhereJSON(field, "containAny", v)
}

// WhereJSONType :
func (q *Query) WhereJSONType(field, typ string) *Query {
	return q.WhereJSON(field, "isType", strings.TrimSpace(strings.ToLower(typ)))
}

// WhereJSONIsObject :
func (q *Query) WhereJSONIsObject(field string) *Query {
	return q.WhereJSON(field, "isObject", nil)
}

// WhereJSONIsArray :
func (q *Query) WhereJSONIsArray(field string) *Query {
	return q.WhereJSON(field, "isArray", nil)
}

// MatchAgainst :
func (q *Query) MatchAgainst(fields []string, values ...string) *Query {
	f := Filter{}
	f.operator = MatchAgainst
	buf := new(bytes.Buffer)
	buf.WriteString("MATCH(")

	for i, field := range fields {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString("`" + field + "`")
	}
	buf.WriteString(") AGAINST(")
	v := ""
	for i := range values {
		if i > 0 {
			buf.WriteByte(' ')
		}

		// buf.WriteString(strconv.Quote(val))
		values[i] = strconv.Quote(values[i])
		if i != len(values)-1 {
			v += strconv.Quote(values[i]) + ","
		} else {
			v += strconv.Quote(values[i])
		}
	}
	buf.WriteString("?)")
	f.raw = buf.String()
	f.value = v
	q.filters = append(q.filters, f)
	return q
}

// Lock :
func (q *Query) Lock(mode locked) *Query {
	q.lockMode = mode
	return q
}

// RLock :
func (q *Query) RLock() *Query {
	q.lockMode = ReadLock
	return q
}

// WLock :
func (q *Query) WLock() *Query {
	q.lockMode = WriteLock
	return q
}

// OrderBy :
// OrderBy(expr.Field("Status", []string{}))
func (q *Query) OrderBy(values ...interface{}) *Query {
	if len(values) <= 0 {
		return q
	}
	for _, v := range values {
		var it interface{}
		switch vi := v.(type) {
		case string:
			sort := expr.Sort{Direction: expr.Ascending}
			if vi[0] == '-' {
				vi = vi[1:]
				sort.Direction = expr.Descending
			}
			sort.Name = vi
			it = sort
		default:
			it = vi
		}
		q.orders = append(q.orders, it)
	}
	// for _, ff := range fields {
	// 	q = q.clone()
	// 	name, dir := strings.TrimSpace(ff), ascending
	// 	if strings.HasPrefix(name, "+") {
	// 		name, dir = strings.TrimSpace(name[1:]), ascending
	// 	} else if strings.HasPrefix(name, "-") {
	// 		name, dir = strings.TrimSpace(name[1:]), descending
	// 	}

	// 	q.orders = append(q.orders, order{
	// 		field:     name,
	// 		direction: dir,
	// 	})
	// }
	return q
}

// Limit :
func (q *Query) Limit(limit int) *Query {
	q.limit = int32(limit)
	return q
}

// Offset :
func (q *Query) Offset(offset int) *Query {
	q.offset = int32(offset)
	return q
}

// ReplaceInto :
func (q *Query) ReplaceInto(ctx context.Context, table string) error {
	return newBuilder(q, operationWrite).replaceInto(ctx, table)
}

// InsertInto :
func (q *Query) InsertInto(ctx context.Context, table string) error {
	return newBuilder(q, operationWrite).insertInto(ctx, table)
}

// Update :
func (q *Query) Update(ctx context.Context, v interface{}) error {
	if err := q.getError(); err != nil {
		return err
	}
	// q = q.OrderBy(pkColumn)
	return newBuilder(q, operationWrite).updateMulti(ctx, v)
}

// Flush :
func (q *Query) Flush(ctx context.Context) error {
	if err := q.getError(); err != nil {
		return err
	}
	if q.table == "" {
		return fmt.Errorf("goloquent: unable to perform delete without table name")
	}
	return newBuilder(q, operationWrite).deleteByQuery(ctx)
}

// Scan :
func (q *Query) Scan(ctx context.Context, dest ...interface{}) error {
	return newBuilder(q, operationRead).scan(ctx, dest...)
}

func (q *Query) InjectResolution(ctx context.Context) context.Context {
	queryScope := extractResolution(ctx)
	queryScope = queryScope.append(q.scope)
	return context.WithValue(ctx, contextResolutionKey, queryScope)
}

func extractResolution(ctx context.Context) scope {
	iQuery := ctx.Value(contextResolutionKey)
	query, ok := iQuery.(scope)
	if ok {
		return query
	}
	return scope{}
}
