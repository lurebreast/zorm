package zorm

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"reflect"
	"runtime"
	"strconv"
	"strings"
)

type ZormEngine struct {
	Db           *sql.DB
	TableName    string
	Prepare      string
	AllExec      []interface{}
	Sql          string
	WhereParam   string
	OrWhereParam string
	LimitParam   string
	OrderParam   string
	WhereExec    []interface{}
	UpdateParam  string
	UpdateExec   []interface{}
	FieldParam   string
	TransStatus  int
	Tx           *sql.Tx
	GroupParam   string
	HavingParam  string
}

// 连接数据库
func NewMysql(address, username, password, dbname string) (*ZormEngine, error) {
	dsn := username + ":" + password + "@tcp(" + address + ")/" + dbname + "?charset=utf8"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(3)

	return &ZormEngine{
		Db:         db,
		FieldParam: "*",
	}, nil
}

// 添加where条件
func (z *ZormEngine) Where(data ...interface{}) *ZormEngine {
	z.whereData("and", data...)
	return z
}

func (z *ZormEngine) OrWhere(data ...interface{}) *ZormEngine {
	z.whereData("or", data...)
	return z
}

func (z *ZormEngine) whereData(whereType string, data ...interface{}) *ZormEngine {
	dataLen := len(data)
	if dataLen != 1 && dataLen != 2 && dataLen != 3 {
		panic("参数个数错误")
	}

	joinOtp := " " + whereType + " "
	if z.WhereParam != "" {
		z.WhereParam += joinOtp
	}

	if dataLen == 1 {
		// 反射type和value
		t := reflect.TypeOf(data[0])
		v := reflect.ValueOf(data[0])

		var fieldName []string

		for i := 0; i < t.NumField(); i++ {
			// 忽略小写不可导出的字段
			if !v.Field(i).CanInterface() {
				continue
			}

			switch t.Field(i).Type.Kind() {
			case reflect.String:
				if v.Field(i).String() == "" {
					continue
				}
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				if v.Field(i).Int() == 0 {
					continue
				}
			}

			sqlTag := t.Field(i).Tag.Get("sql")
			if sqlTag != "" {
				fieldName = append(fieldName, strings.Trim(strings.Split(sqlTag, ",")[0], " ")+"=?")
			} else {
				fieldName = append(fieldName, t.Field(i).Name+"=?")
			}

			z.WhereExec = append(z.WhereExec, v.Field(i).Interface())
		}

		z.WhereParam += strings.Join(fieldName, joinOtp)
	} else if dataLen == 2 {
		z.WhereParam += data[0].(string) + "=?"
		z.WhereExec = append(z.WhereExec, data[1])
	} else if dataLen == 3 {
		whereOpt := strings.ToLower(strings.Trim(data[1].(string), " "))
		if whereOpt == "in" || whereOpt == "not in" {
			valueKind := reflect.TypeOf(data[2]).Kind()
			if valueKind != reflect.Array && valueKind != reflect.Slice {
				panic("in或not in的值必须是数组或slice")
			}

			// 反射值
			v := reflect.ValueOf(data[2])
			dataNum := v.NumField()
			ps := make([]string, dataNum)
			for i := 0; i < dataNum; i++ {
				ps[i] = "?"
				z.WhereExec = append(z.WhereExec, v.Field(i).Interface())
			}

			z.WhereParam += "(" + strings.Join(ps, ",") + ")"

		} else {
			z.WhereParam = data[0].(string) + data[1].(string) + "?"
			z.WhereExec = append(z.WhereExec, data[2])
		}
	}

	return z
}

// 设置数据表
func (z *ZormEngine) Table(tableName string) *ZormEngine {
	z.TableName = tableName

	// 重置引擎
	z.ResetZormEngine()

	return z
}

// 获取表名
func (z *ZormEngine) GetTable() string {
	return z.TableName
}

func (z *ZormEngine) ResetZormEngine() *ZormEngine {
	z.WhereParam = ""
	return z
}

// 单条或批量插入
func (z *ZormEngine) Insert(data interface{}) (int64, error) {
	kind := reflect.ValueOf(data).Kind()
	if kind == reflect.Struct {
		return z.insertData(data, "insert")
	} else if kind == reflect.Array || kind == reflect.Slice {
		return z.batchInsertData(data, "insert")
	} else {
		return 0, errors.New("插入的格式错误，单个为struct, 批量为[]struct")
	}
}

// 单条替换插入
func (z *ZormEngine) Replace(data interface{}) (int64, error) {
	kind := reflect.ValueOf(data).Kind()
	if kind == reflect.Struct {
		return z.insertData(data, "replace")
	} else if kind == reflect.Array || kind == reflect.Slice {
		return z.batchInsertData(data, "replace")
	} else {
		return 0, errors.New("替换插入的格式错误，单个为struct, 批量为[]struct")
	}
}

func (z *ZormEngine) Delete() (int64, error) {
	z.Prepare = "delete from " + z.GetTable()
	if z.WhereParam != "" {
		z.Prepare += " where " + z.WhereParam
	}

	if z.LimitParam != "" {
		z.Prepare += " limit " + z.LimitParam
	}

	var err error
	var stmp *sql.Stmt

	stmp, err = z.Db.Prepare(z.Prepare)
	if err != nil {
		return 0, z.setErrorInfo(err)
	}

	z.AllExec = append(z.AllExec, z.WhereExec...)

	result, err := stmp.Exec(z.AllExec...)
	if err != nil {
		return 0, z.setErrorInfo(err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, z.setErrorInfo(err)
	}

	return rowsAffected, nil
}

func (z *ZormEngine) Update(data ...interface{}) (int64, error) {
	dataLen := len(data)
	if dataLen != 1 && dataLen != 2 {
		panic("更新设置的参数个数错误")
	}

	if dataLen == 1 {
		dataKind := reflect.ValueOf(data[0]).Kind()
		updateFeild := make([]string, 0)
		if dataKind == reflect.Map {
			it := reflect.ValueOf(data[0]).MapRange()
			for it.Next() {
				updateFeild = append(updateFeild, it.Key().String()+"=?")
				z.UpdateExec = append(z.UpdateExec, it.Value().Interface())
			}
			z.UpdateParam = strings.Join(updateFeild, ", ")
		} else if dataKind == reflect.Struct {
			t := reflect.TypeOf(data[0])
			v := reflect.ValueOf(data[0])
			numField := t.NumField()
			for i := 0; i < numField; i++ {
				if !v.Field(i).CanInterface() {
					continue
				}

				sqlTag := t.Field(i).Tag.Get("sql")
				if sqlTag != "" {
					updateFeild = append(updateFeild, strings.Split(sqlTag, ",")[0])
					z.UpdateExec = append(z.UpdateExec, v.Field(i).Interface())
				} else {
					updateFeild = append(updateFeild, t.Field(i).Name)
					z.UpdateExec = append(z.UpdateExec, v.Field(i).Interface())
				}
				z.UpdateParam = strings.Join(updateFeild, ", ")
			}
		} else {
			panic("更新设置的参数必须为map或struct")
		}

	} else if dataLen == 2 {
		z.UpdateParam = data[0].(string) + "=?"
		z.UpdateExec = append(z.UpdateExec, data[1])
	}

	z.Prepare = "update " + z.GetTable() + " set " + z.UpdateParam
	if z.WhereParam != "" {
		z.Prepare += " where " + z.WhereParam
	}

	stmp, err := z.Db.Prepare(z.Prepare)
	if err != nil {
		return 0, z.setErrorInfo(err)
	}

	z.AllExec = append(z.UpdateExec, z.WhereExec...)
	result, err := stmp.Exec(z.AllExec...)
	if err != nil {
		return 0, z.setErrorInfo(err)
	}

	fmt.Println(z.Prepare)

	return result.RowsAffected()
}

func (z *ZormEngine) insertData(data interface{}, insertType string) (int64, error) {
	//反射type和value
	t := reflect.TypeOf(data)
	v := reflect.ValueOf(data)

	// 字段名
	var fieldName []string
	// 点位符?的值
	var placeholder []string
	for i := 0; i < t.NumField(); i++ {
		//小写开头，无法反射，跳过
		if !v.Field(i).CanInterface() {
			continue
		}

		sqlTag := t.Field(i).Tag.Get("sql")
		if sqlTag != "" {
			if strings.Contains(strings.ToLower(sqlTag), "auto_increment") {
				continue
			} else {
				fieldName = append(fieldName, strings.Split(sqlTag, ",")[0])
				placeholder = append(placeholder, "?")
			}
		} else {
			fieldName = append(fieldName, t.Field(i).Name)
			placeholder = append(placeholder, "?")
		}

		z.AllExec = append(z.AllExec, v.Field(i).Interface())
	}

	z.Prepare = insertType + " into " + z.GetTable() + "(`" + strings.Join(fieldName, "`, `") + "`) value(" + strings.Join(placeholder, ", ") + ")"

	stmt, err := z.Db.Prepare(z.Prepare)
	if err != nil {
		return 0, z.setErrorInfo(err)
	}

	result, err := stmt.Exec(z.AllExec...)
	if err != nil {
		return 0, z.setErrorInfo(err)
	}

	return result.LastInsertId()
}

// 批量插入数据
func (z *ZormEngine) batchInsertData(data interface{}, insertType string) (int64, error) {
	getValue := reflect.ValueOf(data)

	if getValue.Kind() != reflect.Slice {
		panic("批量插入的数据类型必须是[]struct")
	}
	l := getValue.Len()

	var fieldName []string
	var placeholderString []string

	for i := 0; i < l; i++ {
		value := getValue.Index(i) // Value of item
		typed := value.Type()      // Type of item

		if typed.Kind() != reflect.Struct {
			panic("批量插入的数据类型必须是[]struct")
		}

		num := value.NumField()

		//子元素值
		var placeholder []string
		for j := 0; j < num; j++ {
			// 小写开头, 无法反射，跳过
			if !value.Field(j).CanInterface() {
				continue
			}

			sqlTag := typed.Field(j).Tag.Get("sql")
			if sqlTag != "" {
				if strings.Contains(sqlTag, "auto_increment") {
					continue
				}
				// 字段只记录第一个
				if i == 0 {
					fieldName = append(fieldName, strings.Split(sqlTag, ",")[0])
				}
				placeholder = append(placeholder, "?")
			} else {
				// 字段只记录第一个
				if i == 0 {
					fieldName = append(fieldName, typed.Field(j).Name)
				}
				placeholder = append(placeholder, "?")
			}

			z.AllExec = append(z.AllExec, value.Field(j).Interface())
		}
		placeholderString = append(placeholderString, "("+strings.Join(placeholder, ", ")+")")
	}

	z.Prepare = insertType + " into " + z.GetTable() + "(`" + strings.Join(fieldName, "`, `") + "`) values " + strings.Join(placeholderString, ", ")
	stmp, err := z.Db.Prepare(z.Prepare)
	if err != nil {
		return 0, z.setErrorInfo(err)
	}

	result, err := stmp.Exec(z.AllExec...)
	if err != nil {
		return 0, z.setErrorInfo(err)
	}

	return result.LastInsertId()
}

func (z *ZormEngine) setErrorInfo(err error) error {
	_, file, line, _ := runtime.Caller(1)
	return errors.New("File: " + file + ":" + strconv.Itoa(line) + ", " + err.Error())
}
