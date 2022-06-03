package main

import (
	"Tcc/Shared/Function"
	"fmt"
)

var ConnectionMongo string = "mongodb://127.0.0.1:27017/"

func main() {
	// Backup validado -> 03/06/2022
	Backup()
}

func Backup() {
	f := Function.TransfereClusters(ConnectionMongo, "Cluster",
		"Identificadores", "Clusters",
		"Backup", "Identificadores1", "Clusters1")

	fmt.Println(f)
}
