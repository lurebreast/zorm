package zorm

import (
	"fmt"
	"testing"
)

var db *ZormEngine

type User struct {
	Uid        int64  `sql:"uid"`
	Username   string `sql:"username" comment:"用户名"`
	Departname string `sql:"departname"`
	//Status     int64  `sql:"status"`
}

func init() {
	var err error
	db, err = NewMysql("127.0.0.1", "root", "Mm122333", "zorm")
	if err != nil {
		fmt.Println("连接数据库异常：" + err.Error())
	}
}

func TestFind(t *testing.T) {
	var result []User

	err := db.Table("userinfo").Where(User{Uid: 4}).Find(&result)
	if err != nil {
		t.Error(err)
	}

	//fmt.Printf("  %%v%v\n  %%+v%+v\n  %%#v%#v\n  ", result, result, result)
}

func TestFindOne(t *testing.T) {
	var result User
	if err := db.Table("userinfo").Where(User{Uid: 4}).FindOne(&result); err != nil {
		t.Error(err)
	}
}

func TestCount(t *testing.T) {
	count, err := db.Table("userinfo").Count()
	fmt.Printf("count %d\n", count)
	if err != nil {
		t.Error(err)
	}
}

func TestSum(t *testing.T) {
	sum, err := db.Table("userinfo").Sum("uid")
	if err != nil {
		t.Error(err)
	}

	fmt.Printf("sum: %v %T\n", sum, sum)
}

func TestGetLastSql(t *testing.T) {
	var result User
	if err := db.Table("userinfo").Where(User{Uid: 4}).FindOne(&result); err != nil {
		t.Error(err)
	}

	sql := db.GetLastSql()
	fmt.Printf("sql: %v\n", sql)
}

func TestExec(t *testing.T) {
	_, err := db.Exec("update userinfo set username=? where uid=?", "hehe", 4)
	if err != nil {
		t.Error(err)
	}
}

func TestQuery(t *testing.T) {
	res, err := db.Query("select * from userinfo where uid=?", 4)
	if err != nil {
		t.Error(err)
	}

	fmt.Printf("res: %#v\n", res)
}

func TestTransaction(t *testing.T) {
	if err := db.Begin(); err != nil {
		t.Error(err)
	}

	_, err := db.Table("userinfo").Where(User{Uid: 4}).Update(map[string]string{"username": "hehe2"})
	if err != nil {
		t.Error(err)
	}

	if err := db.Commit(); err != nil {
		t.Error(err)
	}
}
