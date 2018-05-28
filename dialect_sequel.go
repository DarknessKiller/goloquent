package goloquent

import (
	"bytes"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
)

func checkMultiPtr(v reflect.Value) (isPtr bool, t reflect.Type) {
	t = v.Type().Elem()
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		isPtr = true
	}
	return
}

type sqlCommon interface {
	Prepare(query string) (*sql.Stmt, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

// sequel :
type sequel struct {
	dbName string
	db     sqlCommon
}

var _ Dialect = new(sequel)

func init() {
	RegisterDialect("common", new(sequel))
}

// SetDB :
func (s *sequel) SetDB(db sqlCommon) {
	s.db = db
}

func (s *sequel) Open(conf Config) (*sql.DB, error) {
	connStr := conf.Username + ":" + conf.Password + "@/" + conf.Database
	client, err := sql.Open("common", connStr)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// CreateIndex :
func (s *sequel) CreateIndex(idx string, cols []string) string {
	return fmt.Sprintf("INDEX %s (%s)",
		s.Quote(idx),
		s.Quote(strings.Join(cols, ",")))
}

// GetTable :
func (s *sequel) GetTable(name string) string {
	return fmt.Sprintf("%s.%s", s.Quote(s.dbName), s.Quote(name))
}

// Version :
func (s *sequel) Version() (version string) {
	s.db.QueryRow("SELECT VERSION();").Scan(&version)
	return
}

// CurrentDB :
func (s *sequel) CurrentDB() (name string) {
	if s.dbName != "" {
		name = s.dbName
		return
	}

	s.db.QueryRow("SELECT DATABASE()").Scan(&name)
	s.dbName = name
	return
}

// Quote :
func (s *sequel) Quote(n string) string {
	return fmt.Sprintf("`%s`", n)
}

// Bind :
func (s *sequel) Bind(int) string {
	return "?"
}

// DataType :
func (s *sequel) DataType(sc Schema) string {
	buf := new(bytes.Buffer)
	buf.WriteString(sc.DataType)
	if sc.IsUnsigned {
		buf.WriteString(" UNSIGNED")
	}
	if sc.CharSet != nil {
		buf.WriteString(fmt.Sprintf(" CHARACTER SET %s COLLATE %s",
			s.Quote(sc.CharSet.Encoding),
			s.Quote(sc.CharSet.Collation)))
	}
	if !sc.IsNullable {
		buf.WriteString(" NOT NULL")
		t := reflect.TypeOf(sc.DefaultValue)
		if t != reflect.TypeOf(OmitDefault(nil)) {
			buf.WriteString(fmt.Sprintf(" DEFAULT %s", s.toString(sc.DefaultValue)))
		}
	}
	return buf.String()
}

func (s *sequel) toString(it interface{}) string {
	var v string
	switch vi := it.(type) {
	case string:
		v = fmt.Sprintf("%q", "")
	case bool:
		v = fmt.Sprintf("%t", vi)
	case uint, uint8, uint16, uint32, uint64:
		v = fmt.Sprintf("%d", vi)
	case int, int8, int16, int32, int64:
		v = fmt.Sprintf("%d", vi)
	case float32, float64:
		v = fmt.Sprintf("%v", vi)
	case time.Time:
		v = fmt.Sprintf("%q", vi.Format("2006-01-02 15:04:05"))
	case []interface{}:
		v = fmt.Sprintf("%q", "[]")
	case nil:
		v = "NULL"
	default:
		v = fmt.Sprintf("%v", vi)
	}
	return v
}

// GetSchema :
func (s *sequel) GetSchema(c Column) []Schema {
	f := c.field
	t := f.getRoot().typeOf
	if f.isFlatten() {
		t = f.typeOf
	}

	sc := Schema{
		Name:       c.Name(),
		IsNullable: f.isPtrChild,
	}
	if t.Kind() == reflect.Ptr {
		sc.IsNullable = true
		if t == typeOfPtrKey {
			if f.name == keyFieldName {
				return []Schema{
					Schema{keyColumn, fmt.Sprintf("varchar(%d)", 50), OmitDefault(nil), false, false, false, latin2CharSet},
					Schema{parentColumn, fmt.Sprintf("varchar(%d)", 512), OmitDefault(nil), false, false, false, latin2CharSet},
				}
			}
			sc.IsIndexed = true
			sc.DataType = fmt.Sprintf("varchar(%d)", 512)
			sc.CharSet = latin2CharSet
			return []Schema{sc}
		}
		t = t.Elem()
	}

	switch t {
	case typeOfByte:
		sc.DefaultValue = OmitDefault(nil)
		sc.DataType = "mediumblob"
	case typeOfTime:
		sc.DefaultValue = time.Time{}
		sc.DataType = "datetime"
	case typeOfSoftDelete:
		fmt.Println("SOFTDELETE !!!", c.Name())
		sc.DefaultValue = OmitDefault(nil)
		sc.IsNullable = true
		sc.DataType = "datetime"
	default:
		switch t.Kind() {
		case reflect.String:
			sc.DefaultValue = ""
			sc.DataType = fmt.Sprintf("varchar(%d)", 191)
			if f.isLongText() {
				sc.DefaultValue = nil
				sc.DataType = "text"
			}
			sc.CharSet = utf8mb4CharSet
		case reflect.Bool:
			sc.DefaultValue = false
			sc.DataType = "boolean"
		case reflect.Int8:
			sc.DefaultValue = int8(0)
			sc.DataType = "tinyint"
		case reflect.Int16:
			sc.DefaultValue = int16(0)
			sc.DataType = "mediumint"
		case reflect.Int, reflect.Int32:
			sc.DefaultValue = int(0)
			sc.DataType = "int"
		case reflect.Int64:
			sc.DefaultValue = int64(0)
			sc.DataType = "bigint"
		case reflect.Uint8:
			sc.DefaultValue = uint8(0)
			sc.DataType = "tinyint"
			sc.IsUnsigned = true
		case reflect.Uint, reflect.Uint32:
			sc.DefaultValue = uint(0)
			sc.DataType = "int"
			sc.IsUnsigned = true
		case reflect.Uint64:
			sc.DefaultValue = uint64(0)
			sc.DataType = "bigint"
			sc.IsUnsigned = true
		case reflect.Float32, reflect.Float64:
			sc.DefaultValue = float64(0)
			sc.DataType = "double"
			sc.IsUnsigned = f.isUnsigned()
		case reflect.Slice, reflect.Array:
			sc.DataType = "text"
			sc.DefaultValue = OmitDefault(nil)
			sc.CharSet = utf8CharSet
			if isBaseType(t.Elem()) {
				sc.CharSet = latin2CharSet
			}
			if s.Version() >= "5.5" {
				sc.DataType = "json"
				sc.CharSet = nil
			}
		default:
			sc.DataType = "text"
			sc.DefaultValue = OmitDefault(nil)
			sc.CharSet = utf8CharSet
			if s.Version() >= "5.5" {
				sc.DataType = "json"
				sc.CharSet = nil
			}
		}
	}

	return []Schema{sc}
}

// GetColumns :
func (s *sequel) GetColumns(table string) (columns []string) {
	stmt := "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?;"
	rows, _ := s.db.Query(stmt, s.CurrentDB(), table)
	defer rows.Close()
	for i := 0; rows.Next(); i++ {
		columns = append(columns, "")
		rows.Scan(&columns[i])
	}
	return
}

// GetIndexes :
func (s *sequel) GetIndexes(table string) (idxs []string) {
	stmt := "SELECT DISTINCT INDEX_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME <> ?;"
	rows, _ := s.db.Query(stmt, s.CurrentDB(), table, "PRIMARY")
	defer rows.Close()
	for i := 0; rows.Next(); i++ {
		idxs = append(idxs, "")
		rows.Scan(&idxs[i])
	}
	return
}

func (s *sequel) HasTable(table string) bool {
	var count int
	s.db.QueryRow("SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ? AND table_name = ?", s.CurrentDB(), table).Scan(&count)
	return count > 0
}

// OnConflictUpdate :
func (s *sequel) OnConflictUpdate(cols []string) string {
	buf := new(bytes.Buffer)
	buf.WriteString("ON DUPLICATE KEY UPDATE ")
	for _, c := range cols {
		if c == keyColumn || c == parentColumn {
			continue
		}
		buf.WriteString(fmt.Sprintf("%s=values(%s),",
			s.Quote(c), s.Quote(c)))
	}
	buf.Truncate(buf.Len() - 1)
	return buf.String()
}

// LoadStruct :
func LoadStruct(src interface{}, data map[string]interface{}) error {
	v := reflect.ValueOf(src)
	if v.Type().Kind() != reflect.Ptr {
		return fmt.Errorf("goloquent: struct is not addressable")
	}
	codec, err := getStructCodec(src)
	if err != nil {
		return err
	}

	nv := reflect.New(v.Type().Elem())
	for _, f := range codec.fields {
		fv := getField(nv.Elem(), f.paths)
		if err := loadField(fv, data[f.name]); err != nil {
			return err
		}
	}

	v.Elem().Set(nv.Elem())
	return nil
}

// func (s *sequel) updateMutation(query *Query, model interface{}) (*Stmt, error) {
// 	v := reflect.Indirect(reflect.ValueOf(model))
// 	if v.Len() <= 0 {
// 		return new(Stmt), nil
// 	}

// 	ety, err := newEntity(model)
// 	if err != nil {
// 		return nil, err
// 	}

// 	table := ety.name
// 	buf := new(bytes.Buffer)
// 	args := make([]interface{}, 0)
// 	buf.WriteString(fmt.Sprintf("UPDATE %s SET ", s.Quote(table)))
// 	f := v.Index(0)
// 	data, err := SaveStruct(f.Interface())
// 	if err != nil {
// 		return nil, err
// 	}

// 	pk, isOk := data[keyFieldName].(*datastore.Key)
// 	if !isOk || pk == nil {
// 		return nil, fmt.Errorf("goloquent: entity has no primary key")
// 	}

// 	for k, v := range data {
// 		if k == keyFieldName {
// 			continue
// 		}
// 		it, err := interfaceToValue(v)
// 		if err != nil {
// 			return nil, err
// 		}
// 		it, _ = marshal(it)
// 		buf.WriteString(fmt.Sprintf("%s = %s,", s.Quote(k), s.Bind(0)))
// 		args = append(args, it)
// 	}
// 	buf.Truncate(buf.Len() - 1)
// 	buf.WriteString(fmt.Sprintf(
// 		" WHERE %s = %s AND %s = %s;",
// 		s.Quote(keyColumn), s.Bind(0),
// 		s.Quote(parentColumn), s.Bind(0)))
// 	k, p := splitKey(pk)
// 	args = append(args, k, p)

// 	return &Stmt{
// 		table:     table,
// 		statement: buf,
// 		arguments: args,
// 	}, nil
// }
