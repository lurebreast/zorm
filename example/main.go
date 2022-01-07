package main

import (
	"fmt"
	"github.com/lurebreast/zorm"
)

func main()  {
	type User struct {
		Username   string `sql:"username"`
		Departname string `sql:"departname"`
		Status     int64  `sql:"status"`
	}

	user2 := User{
		Username:   "EE",
		Departname: "22",
		Status:     1,
	}

	db, err := zorm.NewMysql("127.0.0.1", "root", "Mm122333", "zorm")
	if err != nil {
		fmt.Println(err)
		return
	}

	insertId, _ := db.Table("userinfo").Insert(user2)
	fmt.Println(insertId)
}
