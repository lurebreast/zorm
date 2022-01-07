package zorm

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"reflect"
	"strings"
)

type ZormEngine struct {
	Db *sql.DB
	TableName string
	Prepare string
	AllExec []interface{}
	Sql string
	WhereParam string
	OrWhereParam string
	LimitParam string
	OrderParam string
	WhereExec []interface{}
	UpdateParam string
	UpdateExec []interface{}
	FieldParam string
	TransStatus int
	Tx *sql.Tx
	GroupParam string
	HavingParam string
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
func (z *ZormEngine) Where(name string) *ZormEngine {

	return z
}

// 设置数据表
func (z *ZormEngine) Table(tableName string) *ZormEngine  {
	z.TableName = tableName

	// 重置引擎
	z.ResetZormEngine()

	return z
}

// 获取表名
func (z *ZormEngine) GetTable() string  {
	return z.TableName
}

func (z *ZormEngine) ResetZormEngine() *ZormEngine {
	z.WhereParam = ""
	return z
}

func (z *ZormEngine) Insert(data interface{}) (int, error)  {
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

	z.Prepare = "insert into " + z.GetTable() + "(`" + strings.Join(fieldName, "`, `") + "`) value(" + strings.Join(placeholder, ", ")+")"

	fmt.Println(z.Prepare)
	fmt.Println(strings.Title("her royal highness"))

	return 0, nil
}