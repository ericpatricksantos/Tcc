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
		"Testando", "Identificadores", "Clusters")

	fmt.Println(f)
}
