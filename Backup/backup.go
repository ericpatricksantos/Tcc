package main

import (
	"Tcc/Shared/Function"
	"fmt"
)

var ConnectionMongo string = "mongodb://127.0.0.1:27017/"

func main() {
	Backup()
}

func Backup() {
	f := Function.TransfereClusters(ConnectionMongo, "Cluster",
		"Identificadores", "Clusters",
		"Backup", "Identificadores0906", "Clusters0906")

	fmt.Println(f)
}
