package main

import (
	"os"

	"github.com/Mogueno/full-cycle-pix/infrastructure/go"
	"github.com/jinzhu/gorm"
)

var database *gorm.DB

func main() {

	database = db.ConnectDB(os.Getenv("env"))
	grpc.StartGrpcServer(database, 50051)
}
