package Function

import (
	"Tcc/Shared/Database"
	"Tcc/Shared/Model"
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"log"
)

func SalvaCluster(Cluster Model.Clusters, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	if len(Cluster.Identificador) > 0 {
		cliente, contexto, cancel, errou := Database.Connect(ConnectionMongoDB)
		if errou != nil {
			log.Fatal(errou)
		}

		Database.Ping(cliente, contexto)

		Database.ToDoc(Cluster)

		result, err := Database.InsertOne(cliente, contexto, DataBaseMongo, Collection, Cluster)

		// handle the error
		if err != nil {

			panic(err)
		}
		defer Database.Close(cliente, contexto, cancel)

		if result.InsertedID != nil {
			return true
		} else {
			return false
		}

	} else {
		fmt.Println("Cluster invalido: Hash da Transação esta vazio")
		return false
	}

	return false
}

func SalvaIdentificadorCluster(Cluster Model.Identificador, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	if len(Cluster.Identificador) > 0 {
		cliente, contexto, cancel, errou := Database.Connect(ConnectionMongoDB)
		if errou != nil {
			log.Fatal(errou)
		}

		Database.Ping(cliente, contexto)

		Database.ToDoc(Cluster)

		result, err := Database.InsertOne(cliente, contexto, DataBaseMongo, Collection, Cluster)

		// handle the error
		if err != nil {

			panic(err)
		}
		defer Database.Close(cliente, contexto, cancel)

		if result.InsertedID != nil {
			return true
		} else {
			return false
		}

	} else {
		fmt.Println("Cluster invalido: Hash da Transação esta vazio")
		return false
	}

	return false
}

func GetAllClustersLimit(limit int64, ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (Clusters []Model.Clusters) {
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		panic(err)
	}
	defer Database.Close(client, ctx, cancel)
	var filter, option interface{}
	filter = bson.M{}
	option = bson.M{}
	cursor, err := Database.QueryLimit(client, ctx, DataBaseMongo,
		CollectionRecuperaDados, limit, filter, option)

	if err != nil {
		panic(err)
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var cluster Model.Clusters

		if err := cursor.Decode(&cluster); err != nil {
			log.Fatal(err)
		}
		Clusters = append(Clusters, cluster)
	}
	return Clusters
}

func SearchAddrClusters(limit int64, addr string, identificadorAtual, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados string) (result []Model.Clusters) {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		panic(err)
	}

	// Free the resource when mainn dunction is  returned
	defer Database.Close(client, ctx, cancel)

	// create a filter an option of type interface,
	// that stores bjson objects.
	var filter, option interface{}

	// filter  gets all document,
	// with maths field greater that 70
	filter = bson.M{
		"clusters":      addr,
		"identificador": bson.M{"$ne": identificadorAtual},
	}

	option = bson.M{}

	cursor, err := Database.QueryLimit(client, ctx, DataBaseMongo, CollectionRecuperaDados, limit, filter, option)

	// handle the errors.
	if err != nil {

		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var cluster Model.Clusters

		if err := cursor.Decode(&cluster); err != nil {

			fmt.Println()
			fmt.Println("Erro na resposta da função Decode que esta sendo chamada na Função SearchClusters - {Function/Cluster.go}")
			fmt.Println()

			fmt.Println()
			fmt.Println(err.Error())
			fmt.Println()

			log.Fatal(err)
		}

		result = append(result, cluster)

	}

	return result
}

// Map - dicionario

func SalvaMapCluster(Cluster Model.MapCluster, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	if len(Cluster.Identificador) > 0 {
		cliente, contexto, cancel, errou := Database.Connect(ConnectionMongoDB)
		if errou != nil {
			log.Fatal(errou)
		}

		Database.Ping(cliente, contexto)

		Database.ToDoc(Cluster)

		result, err := Database.InsertOne(cliente, contexto, DataBaseMongo, Collection, Cluster)

		// handle the error
		if err != nil {

			panic(err)
		}
		defer Database.Close(cliente, contexto, cancel)

		if result.InsertedID != nil {
			return true
		} else {
			return false
		}

	} else {
		fmt.Println("Cluster invalido: Hash da Transação esta vazio")
		return false
	}

	return false
}

func GetAllMapClustersLimit(limit int64, ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (Clusters []Model.MapCluster) {
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		panic(err)
	}
	defer Database.Close(client, ctx, cancel)
	var filter, option interface{}
	filter = bson.M{}
	option = bson.M{}
	cursor, err := Database.QueryLimit(client, ctx, DataBaseMongo,
		CollectionRecuperaDados, limit, filter, option)

	if err != nil {
		panic(err)
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var cluster Model.MapCluster

		if err := cursor.Decode(&cluster); err != nil {
			log.Fatal(err)
		}
		Clusters = append(Clusters, cluster)
	}
	return Clusters
}

func SearchAddrMapClusters(limit int64, addr string, identificadorAtual, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados string) (result []Model.MapCluster) {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		panic(err)
	}

	// Free the resource when mainn dunction is  returned
	defer Database.Close(client, ctx, cancel)

	// create a filter an option of type interface,
	// that stores bjson objects.
	var filter, option interface{}

	// filter  gets all document,
	// with maths field greater that 70
	filter = bson.M{
		"clusters." + addr: addr,
		"identificador":    bson.M{"$ne": identificadorAtual},
	}

	option = bson.M{}

	cursor, err := Database.QueryLimit(client, ctx, DataBaseMongo, CollectionRecuperaDados, limit, filter, option)

	// handle the errors.
	if err != nil {

		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var cluster Model.MapCluster

		if err := cursor.Decode(&cluster); err != nil {
			log.Fatal(err)
		}

		result = append(result, cluster)

	}

	return result
}
