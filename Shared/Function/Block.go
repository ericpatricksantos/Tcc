package Function

import (
	"fmt"
	"log"
	"Tcc/Shared/Database"
	"Tcc/Shared/Model"

	"gopkg.in/mgo.v2/bson"
)

/*
	Esse arquivo foi criado para armazenar todas as funções que são utilizadas frequentemente.

*/

func GetAllLatestBlock(ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (blocks []Model.LatestBlock) {

	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go}  que esta sendo chamada na Função GetAllLatestBlock - {Function/Block.go}")
		fmt.Println()
		panic(err)
	}

	// Free the resource when mainn dunction is  returned
	defer Database.Close(client, ctx, cancel)

	// create a filter an option of type interface,
	// that stores bjson objects.
	var filter, option interface{}

	// filter  gets all document,
	// with maths field greater that 70
	filter = bson.M{}

	//  option remove id field from all documents
	option = bson.M{}

	// call the query method with client, context,
	// database name, collection  name, filter and option
	// This method returns momngo.cursor and error if any.
	cursor, err := Database.Query(client, ctx, DataBaseMongo,
		CollectionRecuperaDados, filter, option)
	// handle the errors.
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Query - {Database/Mongo.go}  que esta sendo chamada na Função GetAllLatestBlock - {Function/Block.go}")
		fmt.Println()
		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var bloco Model.LatestBlock

		if err := cursor.Decode(&bloco); err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Decode que esta sendo chamada na Função GetAllLatestBlock - {Function/Block.go}")
			fmt.Println()
			log.Fatal(err)
		}

		blocks = append(blocks, bloco)

	}

	return blocks
}

func GetBlock(ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (block Model.LatestBlock) {

	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função GetBlock - {Function/Block.go}")
		fmt.Println()
		panic(err)
	}

	// Free the resource when mainn dunction is  returned
	defer Database.Close(client, ctx, cancel)

	// create a filter an option of type interface,
	// that stores bjson objects.
	var filter, option interface{}

	// filter  gets all document,
	// with maths field greater that 70
	filter = bson.M{}

	//  option remove id field from all documents
	option = bson.M{}

	// call the query method with client, context,
	// database name, collection  name, filter and option
	// This method returns momngo.cursor and error if any.
	cursor, err := Database.Query(client, ctx, DataBaseMongo,
		CollectionRecuperaDados, filter, option)
	// handle the errors.
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Query - {Database/Mongo.go} que esta sendo chamada na Função GetBlock - {Function/Block.go}")
		fmt.Println()
		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		if err := cursor.Decode(&block); err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Decode que esta sendo chamada na Função GetBlock - {Function/Block.go}")
			fmt.Println()
			log.Fatal(err)
		}
		return block
	}

	return block
}

func CheckBlock(ConnectionMongoDB, dataBase, col, key, code string) bool {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função CheckBlock - {Function/Block.go}")
		fmt.Println()
		panic(err)
	}

	// Free the resource when mainn dunction is  returned
	defer Database.Close(client, ctx, cancel)
	count, err := Database.CountElemento(client, ctx, dataBase, col, key, code)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função CountElemento - {Database/Mongo.go} que esta sendo chamada na Função CheckBlock - {Function/Block.go}")
		fmt.Println()
		panic(err)
	}
	if count > 0 {
		return true
	} else {
		return false
	}
}

func SaveLatestBlock(latestBlock Model.LatestBlock,
	ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	if len(latestBlock.TxIndexes) > 0 {
		cliente, contexto, cancel, errou := Database.Connect(ConnectionMongoDB)
		if errou != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função SaveLatestBlock - {Function/Block.go}")
			fmt.Println()
			log.Fatal(errou)
		}

		Database.Ping(cliente, contexto)
		defer Database.Close(cliente, contexto, cancel)

		Database.ToDoc(latestBlock)

		insert, err := Database.InsertOne(cliente, contexto, DataBaseMongo, Collection, latestBlock)

		// handle the error
		if err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função InsertOne - {Database/Mongo.go} que esta sendo chamada na Função SaveLatestBlock - {Function/Block.go}")
			fmt.Println()
			panic(err)
		}

		if insert.InsertedID != nil {
			return true
		} else {
			return false
		}

	} else {
		return false
	}
}

func DeleteLatestBlock(hash string, ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) bool {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função DeleteLatestBlock - {Function/Block.go}")
		fmt.Println()
		panic(err)
	}

	// Free the resource when mainn dunction is  returned
	defer Database.Close(client, ctx, cancel)

	// create a filter an option of type interface,
	// that stores bjson objects.
	var filter interface{}

	// filter  gets all document,
	// with maths field greater that 70
	filter = bson.M{
		"hash": hash,
	}

	cursor, err := Database.DeleteOne(client, ctx, DataBaseMongo, CollectionRecuperaDados, filter)

	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função DeleteOne - {Database/Mongo.go} que esta sendo chamada na Função DeleteLatestBlock - {Function/Block.go}")
		fmt.Println()
		panic(err)
	}
	// verifica a quantidade de linhas afetadas
	if cursor.DeletedCount > 0 {
		return true
	} else {
		return false
	}
}
