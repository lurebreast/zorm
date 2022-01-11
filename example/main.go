package main

import (
	"fmt"
	"github.com/lurebreast/zorm"
)

func main() {
	type User struct {
		Uid        int64  `sql:"uid"`
		Username   string `sql:"username" comment:"用户名"`
		Departname string `sql:"departname"`
		//Status     int64  `sql:"status"`
	}

	db, err := zorm.NewMysql("127.0.0.1", "root", "Mm122333", "zorm")
	if err != nil {
		fmt.Println(err)
		return
	}

	//user2 := User{
	//	Uid: 1,
	//	Username:   "EE",
	//	Departname: "22",
	//	//Status:     1,
	//}
	//insertId, err := db.Table("userinfo").Replace(user2)
	//fmt.Println(insertId, err)

	//var user3 []User
	//user3 = append(user3, User{
	//		Username:   "lisha",
	//		Departname: "开发部",
	//})
	//user3 = append(user3, User{
	//	Username:   "jerry",
	//	Departname: "开发部",
	//})
	//
	//insertId, err := db.Table("userinfo").Insert(user3)
	//fmt.Println(insertId, err)

	//user4 := User{
	//	Uid: 1,
	//	Username:   "EE",
	//	Departname: "22",
	//	//Status:     1,
	//}
	//
	//z := db.Table("userinfo").Where(user4)
	//fmt.Printf("%+v\n", z)

	//user5 := User{
	//	Uid: 3,
	//	Username: "EE",
	//	Departname: "22",
	//}
	//
	//rows, err := db.Table("userinfo").Where(user5).Delete()

	//user6 := User{
	//	Uid: 4,
	//	//Username: "EE3",
	//	//Departname: "22",
	//}
	//rows, err := db.Table("userinfo").Where(user6).Update(map[string]string{"username": "EE5"})
	//fmt.Println(rows, err)

	//user7 := User{
	//	Uid: 4,
	//	//Username: "EE3",
	//	//Departname: "22",
	//}
	//result, err := db.Table("userinfo").Where(user7).Select()
	//fmt.Println(result, err)

}
