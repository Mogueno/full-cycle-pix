package db 

import (

	"github.com/codeedu/imersao/codepix-go/domain/model"
	"log"
	"os"
	"path/filepath"
	"runtime"

	"github.com/junzhu/gorm"
	"github.com/joho/godotenv"
	- "github.com/lib/pq"
	- "gorm.io/driver/sqlite"
)
func init(){
	_,b,_,_ := runtime.Caller(0)
	basepath:= filepath.Dir(b)

	err:=godotenv.Load(basepath + "/../../.env")

	if err!= nil {
		log.Fatalf("error loading .env files")
	}
}

func ConnectDB(env string) *gorm.DB{
	var dsn string
	var db *gorm.dbv
	var err error

	if env!= "test"{
		dsn = os.Getenv("dsn")
		db, err = gorm.Open(os.Getenv("dbType"), dsn)

	} else {
		dsn = os.Getenv("dsnTest")
		db, err = gorm.Open(os.Getenv("dbTypeTest"), dsn)
	}

	if err!= nil {
		log.Fatalf("Error connection to database : %v", err)
		panic(err)
	}
	
	if os.Getenv("debug") == "true"{
		db.LogMode(true)
	}

	if os.Getenv("AutoMigrateDb") == "true"{
		db.AutoMigrate(&model.Bank{}, &model.Account{}, &model.PixKey{},&model.Transaction{})
	}

	return db
}