package zorm

import (
	"database/sql"
	"errors"
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
	z.WhereExec = nil
	return z
}

// 单条或批量插入
func (z *ZormEngine) Insert(data interface{}) (int64, error) {
	kind := reflect.ValueOf(data).Kind()
	if kind == reflect.Struct {
		return z.insertData(data, "INSERT")
	} else if kind == reflect.Array || kind == reflect.Slice {
		return z.batchInsertData(data, "INSERT")
	} else {
		return 0, errors.New("插入的格式错误，单个为struct, 批量为[]struct")
	}
}

// 单条替换插入
func (z *ZormEngine) Replace(data interface{}) (int64, error) {
	kind := reflect.ValueOf(data).Kind()
	if kind == reflect.Struct {
		return z.insertData(data, "REPLACE")
	} else if kind == reflect.Array || kind == reflect.Slice {
		return z.batchInsertData(data, "REPLACE")
	} else {
		return 0, errors.New("替换插入的格式错误，单个为struct, 批量为[]struct")
	}
}

func (z *ZormEngine) Delete() (int64, error) {
	z.Prepare = "DELETE FROM " + z.GetTable()
	if z.WhereParam != "" {
		z.Prepare += " WHERE " + z.WhereParam
	}

	if z.LimitParam != "" {
		z.Prepare += " LIMIT " + z.LimitParam
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

	z.Prepare = "UPDATE " + z.GetTable() + " SET " + z.UpdateParam
	if z.WhereParam != "" {
		z.Prepare += " WHERE " + z.WhereParam
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

	return result.RowsAffected()
}

// 查询
func (z *ZormEngine) Select() ([]map[string]string, error) {
	z.Prepare = "SELECT " + z.FieldParam + " FROM " + z.GetTable()
	if z.WhereParam != "" {
		z.Prepare += " WHERE " + z.WhereParam
	}
	if z.LimitParam != "" {
		z.Prepare += " LIMIT " + z.LimitParam
	}

	z.AllExec = z.WhereExec
	rows, err := z.Db.Query(z.Prepare, z.AllExec...)
	if err != nil {
		return nil, z.setErrorInfo(err)
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, z.setErrorInfo(err)
	}

	values := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for i := range values {
		scans[i] = &values[i]
	}

	result := make([]map[string]string, 0)
	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			return nil, z.setErrorInfo(err)
		}

		row := make(map[string]string)
		for k, v := range values {
			key := columns[k]
			row[key] = string(v)
		}

		result = append(result, row)
	}

	return result, nil
}

func (z *ZormEngine) SelectOne() (map[string]string, error) {
	result, err := z.Limit(1).Select()
	if err != nil {
		return nil, z.setErrorInfo(err)
	}

	return result[0], nil
}

func (z *ZormEngine) Find(result interface{}) error {
	z.Prepare = "SELECT " + z.FieldParam + " FROM " + z.GetTable()
	if z.WhereParam != "" {
		z.Prepare += " WHERE " + z.WhereParam
	}
	if z.LimitParam != "" {
		z.Prepare += " LIMIT " + z.LimitParam
	}

	z.AllExec = z.WhereExec
	rows, err := z.Db.Query(z.Prepare, z.AllExec...)

	//fmt.Printf("prepare %+v allExec %#v\n", z.Prepare, z.AllExec)

	if err != nil {
		return z.setErrorInfo(err)
	}

	columns, err := rows.Columns()
	if err != nil {
		return z.setErrorInfo(err)
	}

	values := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for i := range values {
		scans[i] = &values[i]
	}

	destSlice := reflect.ValueOf(result).Elem()
	destType := destSlice.Type().Elem()

	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			return z.setErrorInfo(err)
		}

		dest := reflect.New(destType).Elem()

		for k, v := range values {
			key := columns[k]
			value := string(v)

			// 遍历结构体
			for i := 0; i < destType.NumField(); i++ {
				// 字段
				var fieldName string
				sqlTag := destType.Field(i).Tag.Get("sql")
				if sqlTag != "" {
					fieldName = strings.Split(sqlTag, ",")[0]
				} else {
					fieldName = destType.Field(i).Name
				}

				// 返回的字段不在结构体中，跳过
				if key != fieldName {
					continue
				}

				// 反射赋值
				if err := z.reflectSet(dest, i, value); err != nil {
					return err
				}
			}
		}

		// 追加到结果中
		destSlice.Set(reflect.Append(destSlice, dest))
	}

	return nil
}

// 单条查找
func (z *ZormEngine) FindOne(result interface{}) error {
	//取的原始值
	dest := reflect.Indirect(reflect.ValueOf(result))

	//new一个类型的切片
	destSlice := reflect.New(reflect.SliceOf(dest.Type())).Elem()

	//调用
	if err := z.Limit(1).Find(destSlice.Addr().Interface()); err != nil {
		return err
	}

	//判断返回值长度
	if destSlice.Len() == 0 {
		return z.setErrorInfo(errors.New("数据未找到"))
	}

	dest.Set(destSlice.Index(0))
	return nil
}

// 总记录数
func (z *ZormEngine) Count() (int64, error) {
	res, err := z.aggregateQuery("COUNT", "*")
	if err != nil {
		return 0, z.setErrorInfo(err)
	}

	s := string(res.([]byte))
	num, _ := strconv.ParseInt(s, 10, 64)

	return num, nil
}

//总和
func (z *ZormEngine) Sum(param string) (string, error) {
	sum, err := z.aggregateQuery("sum", param)
	if err != nil {
		return "0", z.setErrorInfo(err)
	}
	return string(sum.([]byte)), nil
}

func (z *ZormEngine) aggregateQuery(funcName, field string) (interface{}, error) {
	z.Prepare = "SELECT " + funcName + "(" + field + ") FROM " + z.GetTable()
	if z.WhereParam != "" {
		z.Prepare += " WHERE " + z.WhereParam
	}

	z.AllExec = z.WhereExec
	var res interface{}
	if err := z.Db.QueryRow(z.Prepare, z.AllExec...).Scan(&res); err != nil {
		return nil, z.setErrorInfo(err)
	}

	return res, nil
}

func (z *ZormEngine) reflectSet(dest reflect.Value, i int, value string) error {
	switch dest.Field(i).Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		num, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		dest.Field(i).SetInt(num)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		num, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return err
		}
		dest.Field(i).SetUint(num)
	case reflect.Float32:
		res, err := strconv.ParseFloat(value, 32)
		if err != nil {
			return err
		}
		dest.Field(i).SetFloat(res)
	case reflect.Float64:
		res, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return err
		}
		dest.Field(i).SetFloat(res)
	case reflect.Bool:
		res, err := strconv.ParseBool(value)
		if err != nil {
			return err
		}
		dest.Field(i).SetBool(res)
	case reflect.String:
		dest.Field(i).SetString(value)
	}

	return nil
}

// 排序
func (z *ZormEngine) Order(order string) *ZormEngine {
	if z.OrderParam != "" {
		z.OrderParam += ", "
	}
	z.OrderParam = order
	return z
}

// 限制条数
// Usage:
//     Limit(1, 2)
func (z *ZormEngine) Limit(limit ...int) *ZormEngine {
	if len(limit) == 1 {
		z.LimitParam = strconv.Itoa(limit[0])
	} else if len(limit) == 2 {
		z.LimitParam = strconv.Itoa(limit[0]) + " OFFSET " + strconv.Itoa(limit[1])
	} else {
		panic("参数个数错误")
	}

	return z
}

// 生成sql
func (z *ZormEngine) GetLastSql() string {
	z.generateSql()
	return z.Sql
}

func (z *ZormEngine) generateSql() {
	z.Sql = z.Prepare
	for _, v := range z.AllExec {
		switch v.(type) {
		case int:
			z.Sql = strings.Replace(z.Sql, "?", strconv.Itoa(v.(int)), 1)
		case int64:
			z.Sql = strings.Replace(z.Sql, "?", strconv.FormatInt(v.(int64), 10), 1)
		case bool:
			z.Sql = strings.Replace(z.Sql, "?", strconv.FormatBool(v.(bool)), 1)
		default:
			z.Sql = strings.Replace(z.Sql, "?", "'"+v.(string)+"'", 1)
		}
	}
}

func (z *ZormEngine) Exec(query string, args ...interface{}) (int64, error) {
	var err error
	var res int64
	var result sql.Result

	result, err = z.Db.Exec(query, args...)
	if err != nil {
		return 0, z.setErrorInfo(err)
	}

	if strings.Contains(strings.ToLower(query), "insert") {
		res, err = result.LastInsertId()
	} else {
		res, err = result.RowsAffected()
	}

	return res, err
}

// 原生sql查询
func (z *ZormEngine) Query(query string, args ...interface{}) ([]map[string]string, error) {
	rows, err := z.Db.Query(query, args...)
	if err != nil {
		return nil, z.setErrorInfo(err)
	}

	columns, _ := rows.Columns()
	values := make([][]byte, len(columns))
	scans := make([]interface{}, len(columns))
	for index := range values {
		scans[index] = &values[index]
	}

	result := make([]map[string]string, 0)
	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			return nil, z.setErrorInfo(err)
		}

		row := make(map[string]string)
		for k, v := range values {
			key := columns[k]
			row[key] = string(v)
		}

		result = append(result, row)
	}

	return result, nil
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
				if strings.Contains(strings.ToLower(sqlTag), "auto_increment") {
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

	z.Prepare = insertType + " INTO " + z.GetTable() + "(`" + strings.Join(fieldName, "`, `") + "`) VALUES " + strings.Join(placeholderString, ", ")
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
