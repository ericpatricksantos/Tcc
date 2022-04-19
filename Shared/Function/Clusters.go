package Function

import (
	"Tcc/Shared/Database"
	"Tcc/Shared/Model"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
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

	// Essa parte esta comentada, pq o valor dos dicionario nao foi preenchido
	//filter = bson.M{
	//	"clusters." + addr: addr,
	//	"identificador":    bson.M{"$ne": identificadorAtual},
	//}
	filter = bson.M{
		"clusters." + addr: "",
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

func SearchAddrMapsClusters(limit int64, identificadoresPesquisados []string, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados string) (result []Model.MapCluster) {
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
		"identificador": bson.M{"$in": identificadoresPesquisados},
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

func DeleteIdentificadoresCluster(identificador string, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
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
		"identificador": identificador,
	}

	cursor, err := Database.DeleteMany(client, ctx, DataBaseMongo, Collection, filter)

	if err != nil {

		panic(err)
	}
	// verifica a quantidade de linhas afetadas
	if cursor.DeletedCount > 0 {
		return true
	} else {
		return false
	}
}

func DeleleListIdentificadoresCluster(identificadores []string, ConnectionMongoDB, DataBaseMongo, Collection string) (sucesso bool) {
	for _, elem := range identificadores {
		confirm := DeleteIdentificadoresCluster(elem, ConnectionMongoDB, DataBaseMongo, Collection)
		if !confirm {
			fmt.Println(" Erro: Não foi Deletado os identificadores")
			return false
		}
	}
	fmt.Println(" Deletado os identificadores com Sucesso")
	return true
}

func DeleleListIdentificadoresAndClusters(identificadores []string, ConnectionMongoDB, DataBaseMongo, Collection_Identificadores, Collection_Map_Clusters string) (sucesso bool) {
	var sucess bool
	for _, elem := range identificadores {
		sucess = DeleteIdentificadoresCluster(elem, ConnectionMongoDB, DataBaseMongo, Collection_Identificadores)
		if sucess {
			sucess = DeleteIdentificadoresCluster(elem, ConnectionMongoDB, DataBaseMongo, Collection_Map_Clusters)
			if !sucess {
				fmt.Println(" Erro: Não foi Deletado os Clusters")
				return false
			}
		} else {
			fmt.Println(" Erro: Não foi Deletado os identificadores")
			return false
		}
	}
	fmt.Println(" Deletado os identificadores e Clusters com Sucesso")
	return true
}

func PutIdentificador(identificadorBase, identificadorModificado, ConnectionMongoDB, DataBaseMongo, Collection string) bool {

	cliente, contexto, cancel, errou := Database.Connect(ConnectionMongoDB)
	if errou != nil {
		return false
	}

	Database.Ping(cliente, contexto)
	defer Database.Close(cliente, contexto, cancel)

	var result *mongo.UpdateResult
	var err error

	filter := bson.M{
		"identificador": identificadorModificado,
	}
	update := bson.M{"$set": bson.M{"identificador": identificadorBase}}

	result, err = Database.UpdateMany(cliente, contexto, DataBaseMongo, Collection, filter, update)

	// handle the error
	if err != nil {
		return false
	}

	if result.ModifiedCount > 0 {
		return true
	} else {
		return false
	}
}

func PutTamanhoCluster(tamanho_map_enderecos_resultante int, identificadorBase, ConnectionMongoDB, DataBaseMongo, Collection string) bool {

	cliente, contexto, cancel, errou := Database.Connect(ConnectionMongoDB)
	if errou != nil {
		return false
	}

	Database.Ping(cliente, contexto)
	defer Database.Close(cliente, contexto, cancel)

	var result *mongo.UpdateResult
	var err error

	filter := bson.M{
		"identificador": identificadorBase,
	}
	update := bson.M{"$set": bson.M{"tamanhocluster": tamanho_map_enderecos_resultante}}

	result, err = Database.UpdateOne(cliente, contexto, DataBaseMongo, Collection, filter, update)

	// handle the error
	if err != nil {
		return false
	}

	if result.ModifiedCount > 0 {
		return true
	} else {
		return false
	}
}

func PutMapCluster(clusterResultante map[string]string, identificador, ConnectionMongoDB, DataBaseMongo, Collection string) bool {
	cliente, contexto, cancel, errou := Database.Connect(ConnectionMongoDB)
	if errou != nil {
		return false
	}

	Database.Ping(cliente, contexto)
	defer Database.Close(cliente, contexto, cancel)

	var result *mongo.UpdateResult
	var err error

	filter := bson.M{
		"identificador": identificador,
	}
	update := bson.M{"$set": bson.M{"clusters": clusterResultante}}

	result, err = Database.UpdateOne(cliente, contexto, DataBaseMongo, Collection, filter, update)

	// handle the error
	if err != nil {
		return false
	}

	if result.ModifiedCount > 0 {
		return true
	} else {
		return false
	}
}

// Identificadores

func GetIdentificadorById(identificador string, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados string) (result Model.Identificador) {
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
		"identificador": identificador,
	}

	option = bson.M{}

	cursor, err := Database.QueryLimit(client, ctx, DataBaseMongo, CollectionRecuperaDados, 1, filter, option)

	// handle the errors.
	if err != nil {

		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var identificador_cluster Model.Identificador

		if err := cursor.Decode(&identificador_cluster); err != nil {
			log.Fatal(err)
		}
		return identificador_cluster
	}

	return result
}
