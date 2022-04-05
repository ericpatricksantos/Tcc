package Function

import (
	"fmt"
	"log"
	"Tcc/Shared/Database"
	"Tcc/Shared/Model"

	"gopkg.in/mgo.v2/bson"
)

func SaveTx(Tx Model.Transaction, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	if len(Tx.Hash) > 0 {
		cliente, contexto, cancel, errou := Database.Connect(ConnectionMongoDB)
		if errou != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go}  que esta sendo chamada na Função SaveTx - {Function/Tx.go}")
			fmt.Println()

			panic(errou)
		}

		Database.Ping(cliente, contexto)
		defer Database.Close(cliente, contexto, cancel)

		Database.ToDoc(Tx)

		result, err := Database.InsertOne(cliente, contexto, DataBaseMongo, Collection, Tx)

		// handle the error
		if err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função InsertOne - {Database/Mongo.go}  que esta sendo chamada na Função SaveTx - {Function/Tx.go}")
			fmt.Println()

			panic(err)
		}

		if result.InsertedID != nil {
			return true
		} else {
			return false
		}

	} else {
		fmt.Println("O hash dessa Transação esta vazio -", Tx.Hash, "-")
		return false
	}
}

func GetAllTxs(ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (Txs []Model.Transaction) {

	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go}  que esta sendo chamada na Função GetAllTxs - {Function/Tx.go}")
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
		fmt.Println("Erro na resposta da função Query - {Database/Mongo.go}  que esta sendo chamada na Função GetAllTxs - {Function/Tx.go}")
		fmt.Println()

		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var transaction Model.Transaction

		if err := cursor.Decode(&transaction); err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Decode que esta sendo chamada na Função GetAllTxs - {Function/Tx.go}")
			fmt.Println()

			log.Fatal(err)
		}

		Txs = append(Txs, transaction)

	}

	return Txs
}

func DeleteTx(hash string, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função DeleteTx - {Function/Tx.go}")
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

	cursor, err := Database.DeleteOne(client, ctx, DataBaseMongo, Collection, filter)

	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função DeleteOne - {Database/Mongo.go} que esta sendo chamada na Função DeleteTx - {Function/Tx.go}")
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

func GetTxMongoDB(ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (tx Model.Transaction) {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go}  que esta sendo chamada na Função GetTxMongoDB - {Function/Tx.go}")
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
		fmt.Println("Erro na resposta da função Query - {Database/Mongo.go}  que esta sendo chamada na Função GetTxMongoDB - {Function/Tx.go}")
		fmt.Println()

		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {

		if err := cursor.Decode(&tx); err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Decode que esta sendo chamada na Função GetTxMongoDB - {Function/Tx.go}")
			fmt.Println()

			log.Fatal(err)
		}

		return tx
	}

	return Model.Transaction{}
}

func CheckTx(ConnectionMongoDB, dataBase, col, key, code string) bool {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função CheckTx - {Function/Tx.go}")
		fmt.Println()
		panic(err)
	}

	// Free the resource when mainn dunction is  returned
	defer Database.Close(client, ctx, cancel)
	count, err := Database.CountElemento(client, ctx, dataBase, col, key, code)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função CountElemento - {Database/Mongo.go} que esta sendo chamada na Função CheckTx - {Function/Tx.go}")
		fmt.Println()
		panic(err)
	}
	if count > 0 {
		return true
	} else {
		return false
	}
}

func CheckTxIndex(ConnectionMongoDB, dataBase, col, key string, code int) bool {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função CheckTxIndex - {Function/Tx.go}")
		fmt.Println()
		panic(err)
	}

	// Free the resource when mainn dunction is  returned
	defer Database.Close(client, ctx, cancel)
	count, err := Database.CountElementoTxIndex(client, ctx, dataBase, col, key, code)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função CountElementoTxIndex - {Database/Mongo.go} que esta sendo chamada na Função CheckTxIndex - {Function/Tx.go}")
		fmt.Println()
		panic(err)
	}
	if count > 0 {
		return true
	} else {
		return false
	}
}

func SalveTxMongoDB(tx Model.Transaction, ConnectionMongoDB, DataBaseMongo, Collection string) (salvo bool, existente bool) {
	confirm := CheckTx(ConnectionMongoDB, DataBaseMongo, Collection, "hash", tx.Hash)
	if confirm {
		fmt.Println("Esse tx ja existe nessa Collection: ", Collection)
		return false, true
	}
	return SaveTx(tx, ConnectionMongoDB, DataBaseMongo, Collection), false
}

func DeleteTxMongo(hash string, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	confirm := CheckTx(ConnectionMongoDB, DataBaseMongo, Collection, "hash", hash)
	if !confirm {
		fmt.Println("Esse tx não existe nessa Collection, por isso não tem como excluir: ", Collection)
		return false
	}
	return DeleteTx(hash, ConnectionMongoDB, DataBaseMongo, Collection)
}

func MudancaStatusTx(tx Model.Transaction, ConnectionMongoDB, DataBaseTx, collectionOrigem, collectionDestino string) bool {

	salvo, existente := SalveTxMongoDB(tx, ConnectionMongoDB, DataBaseTx, collectionDestino)

	if !salvo && !existente {
		fmt.Println("Não foi salvo com Sucesso a tx na collection ", collectionDestino)
		return false
	} else if !salvo && existente {
		fmt.Println("Essa tx ja existente na collection ", collectionDestino)
	} else {
		fmt.Println("Salvo com sucesso a tx na collection ", collectionDestino)
	}

	deletado := DeleteTxMongo(tx.Hash, ConnectionMongoDB, DataBaseTx, collectionOrigem)

	if !deletado {
		fmt.Println("Hash: ", tx.Hash, " não foi deletado de ", collectionOrigem)
		return false
	} else {
		fmt.Println("Deletado com sucesso a tx da collection", collectionOrigem)
	}

	return true
}
