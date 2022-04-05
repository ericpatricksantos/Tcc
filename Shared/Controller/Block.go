package Controller

import (
	"fmt"
	"Tcc/Shared/API"
	"Tcc/Shared/Function"
	"Tcc/Shared/Model"
)

func GetAllLatestBlock(ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) []Model.LatestBlock {
	return Function.GetAllLatestBlock(ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)
}

func SaveLatestBlock(UrlAPI string, LatestBlock string, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	ultimoBloco := GetLatestBlock(UrlAPI, LatestBlock)
	conf := Function.CheckBlock(ConnectionMongoDB, DataBaseMongo, Collection, "hash", ultimoBloco.Hash)
	if conf {
		fmt.Println("Esse bloco foi salvo anteriormente")
		return false
	} else {
		resposta := Function.SaveLatestBlock(ultimoBloco, ConnectionMongoDB, DataBaseMongo, Collection)
		if resposta {
			fmt.Println("Ultimo Bloco Salvo com Sucesso")
			return true
		} else {
			return false
		}

	}
}

func SaveBlock(latestBlock Model.LatestBlock, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	return Function.SaveLatestBlock(latestBlock, ConnectionMongoDB, DataBaseMongo, Collection)
}

func GetLatestBlock(UrlAPI string, LatestBlock string) Model.LatestBlock {
	return API.GetLatestBlock(UrlAPI, LatestBlock)
}

func DeleteLatestBlock(hash string, ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) bool {
	return Function.DeleteLatestBlock(hash, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)
}

func GetBlock(ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (block Model.LatestBlock) {
	return Function.GetBlock(ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)
}
