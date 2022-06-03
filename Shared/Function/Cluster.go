package Function

import (
	"Tcc/Shared/Database"
	"Tcc/Shared/Model"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"gopkg.in/mgo.v2/bson"
	"log"
	"time"
)

func GetAllCluster(ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (Clusters []Model.Cluster) {

	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função GetAllCluster - {Function/Cluster.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
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
		fmt.Println("Erro na resposta da função Query - {Database/Mongo.go} que esta sendo chamada na Função GetAllCluster - {Function/Cluster.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var cluster Model.Cluster

		if err := cursor.Decode(&cluster); err != nil {

			fmt.Println()
			fmt.Println("Erro na resposta da função Decode que esta sendo chamada na Função GetAllCluster - {Function/Cluster.go}")
			fmt.Println()

			fmt.Println()
			fmt.Println(err.Error())
			fmt.Println()

			log.Fatal(err)
		}

		Clusters = append(Clusters, cluster)

	}

	return Clusters
}

func GetAllClusterLimit(limit int64, ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (Clusters []Model.Cluster) {
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
		var cluster Model.Cluster

		if err := cursor.Decode(&cluster); err != nil {
			log.Fatal(err)
		}
		Clusters = append(Clusters, cluster)
	}
	return Clusters
}

func GetAllClusterLimitOffset(limit int64, offset int64, ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (Clusters []Model.Cluster) {
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		panic(err)
	}
	defer Database.Close(client, ctx, cancel)
	var filter, option interface{}
	filter = bson.M{}
	option = bson.M{}
	cursor, err := Database.QueryLimitOffset(client, ctx, DataBaseMongo,
		CollectionRecuperaDados, limit, offset, filter, option)

	if err != nil {
		panic(err)
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var cluster Model.Cluster

		if err := cursor.Decode(&cluster); err != nil {
			log.Fatal(err)
		}
		Clusters = append(Clusters, cluster)
	}
	return Clusters
}

func GetAllClusterLimitOffsetToDocs(limit int64, offset int64, ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (Clusters []interface{}) {
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		panic(err)
	}
	defer Database.Close(client, ctx, cancel)
	var filter, option interface{}
	filter = bson.M{}
	option = bson.M{}
	cursor, err := Database.QueryLimitOffset(client, ctx, DataBaseMongo,
		CollectionRecuperaDados, limit, offset, filter, option)

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

func GetAllIdentificadoresLimitOffsetToDocs(limit int64, offset int64, ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (Clusters []interface{}) {
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		panic(err)
	}
	defer Database.Close(client, ctx, cancel)
	var filter, option interface{}
	filter = bson.M{}
	option = bson.M{}
	cursor, err := Database.QueryLimitOffset(client, ctx, DataBaseMongo,
		CollectionRecuperaDados, limit, offset, filter, option)

	if err != nil {
		panic(err)
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var cluster Model.Identificador

		if err := cursor.Decode(&cluster); err != nil {
			log.Fatal(err)
		}
		Clusters = append(Clusters, cluster)
	}
	return Clusters
}

func SearchClusters(addr string, ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (result []Model.Cluster) {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função SearchClusters - {Function/Cluster.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
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
	filter = bson.M{
		"input": addr,
	}

	option = bson.M{}

	cursor, err := Database.QueryLimit(client, ctx, DataBaseMongo, CollectionRecuperaDados, 5, filter, option)

	// handle the errors.
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função Query - {Database/Mongo.go} que esta sendo chamada na Função SearchClusters - {Function/Cluster.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var cluster Model.Cluster

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

func SearchClustersLimit(limit int64, addrInput, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados string) (result []Model.Cluster) {

	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		panic(err)
	}

	defer Database.Close(client, ctx, cancel)
	var filter, option interface{}
	filter = bson.M{
		"input": addrInput,
	}

	option = bson.M{}

	cursor, err := Database.QueryLimit(client, ctx, DataBaseMongo, CollectionRecuperaDados, limit, filter, option)
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função Query - {Database/Mongo.go} que esta sendo chamada na Função SearchClusters - {Function/Cluster.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		panic(err)
	}
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var cluster Model.Cluster

		if err := cursor.Decode(&cluster); err != nil {
			log.Fatal(err)
		}
		result = append(result, cluster)

	}
	return result
}

func GetClusterByIdenficador(addr string, ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (result Model.Cluster) {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função SearchClusters - {Function/Cluster.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
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
	filter = bson.M{
		"hash": addr,
	}

	option = bson.M{}

	cursor, err := Database.QueryLimit(client, ctx, DataBaseMongo, CollectionRecuperaDados, 5, filter, option)

	// handle the errors.
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função Query - {Database/Mongo.go} que esta sendo chamada na Função SearchClusters - {Function/Cluster.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {

		if err := cursor.Decode(&result); err != nil {

			fmt.Println()
			fmt.Println("Erro na resposta da função Decode que esta sendo chamada na Função SearchClusters - {Function/Cluster.go}")
			fmt.Println()

			fmt.Println()
			fmt.Println(err.Error())
			fmt.Println()

			log.Fatal(err)
		}

		return result

	}

	return result
}

func GetClustersByIdentificador(addr string, ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (result []Model.Cluster) {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função SearchClusters - {Function/Cluster.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
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
	filter = bson.M{
		"hash": addr,
	}

	option = bson.M{}

	cursor, err := Database.QueryLimit(client, ctx, DataBaseMongo, CollectionRecuperaDados, 5, filter, option)

	// handle the errors.
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função Query - {Database/Mongo.go} que esta sendo chamada na Função SearchClusters - {Function/Cluster.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		cluster := Model.Cluster{}
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
		return result

	}

	return result
}

func DeleteCluster(hash string, ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) bool {
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função DeleteCluster - {Function/Cluster.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		return false
	}

	var filter interface{}
	filter = bson.M{
		"hash": hash,
	}

	cursor, err := Database.DeleteOne(client, ctx, DataBaseMongo, CollectionRecuperaDados, filter)

	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função DeleteOne - {Database/Mongo.go} que esta sendo chamada na Função DeleteCluster - {Function/Cluster.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		return false
	}

	defer Database.Close(client, ctx, cancel)

	// verifica a quantidade de linhas afetadas
	if cursor.DeletedCount > 0 {

		return true
	} else {

		return false
	}
}

func SaveCluster(Cluster Model.Cluster, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	if len(Cluster.Hash) > 0 {
		cliente, contexto, cancel, errou := Database.Connect(ConnectionMongoDB)
		if errou != nil {

			fmt.Println()
			fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função SaveCluster - {Function/Cluster.go}")
			fmt.Println()

			fmt.Println()
			fmt.Println(errou.Error())
			fmt.Println()

			log.Fatal(errou)
		}

		Database.Ping(cliente, contexto)

		Database.ToDoc(Cluster)

		result, err := Database.InsertOne(cliente, contexto, DataBaseMongo, Collection, Cluster)

		// handle the error
		if err != nil {

			fmt.Println()
			fmt.Println("Erro na resposta da função InsertOne - {Database/Mongo.go} que esta sendo chamada na Função SaveCluster - {Function/Cluster.go}")
			fmt.Println()

			fmt.Println()
			fmt.Println(errou.Error())
			fmt.Println()

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

func PutListCluster(Hash string, Input []string, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	tamanhoClusterResultante := len(Input)
	for index, item := range Input {
		fmt.Println("Tamanho do Cluster Resultante: ", tamanhoClusterResultante)
		fmt.Println("Indice do Cluster Resultante: ", index)
		fmt.Println("Adicionando o addr: ", item, " no cluster: ", Hash)

		PutCluster(Hash, item, ConnectionMongoDB, DataBaseMongo, Collection)

		fmt.Println("Adicionado com Sucesso o addr:", item, " no cluster: ", Hash)
		if tamanhoClusterResultante > 100 {
			time.Sleep(time.Second * time.Duration(1))
		}
	}
	fmt.Println()
	return true
}

func PutCluster(Hash string, Input string, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	if len(Input) > 0 {
		cliente, contexto, cancel, errou := Database.Connect(ConnectionMongoDB)
		if errou != nil {

			fmt.Println()
			fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função PutCluster - {Function/Cluster.go}")
			fmt.Println()

			fmt.Println()
			fmt.Println(errou.Error())
			fmt.Println()

			log.Fatal(errou)
		}

		Database.Ping(cliente, contexto)
		defer Database.Close(cliente, contexto, cancel)

		var result *mongo.UpdateResult
		var err error

		filter := bson.M{
			"hash": Hash,
		}
		update := bson.M{"$addToSet": bson.M{"input": Input}}

		result, err = Database.UpdateOne(cliente, contexto, DataBaseMongo, Collection, filter, update)

		// handle the error
		if err != nil {

			fmt.Println()
			fmt.Println("Erro na resposta da função UpdateOne - {Database/Mongo.go} que esta sendo chamada na Função PutCluster - {Function/Cluster.go}")
			fmt.Println()

			fmt.Println()
			fmt.Println(errou.Error())
			fmt.Println()
			panic(err)
		}

		if result.ModifiedCount > 0 {
			return true
		} else {
			return false
		}
	} else {
		fmt.Println("O Hash do Cluster esta vazio")
		return false
	}
	return false
}

func AddAll(Hash string, Input []string, ConnectionMongoDB string, DataBaseMongo string, Collection string) (sucesso, erro bool) {
	matriz, tam := TransformaArrayEmMatriz(Input, 0, 300)
	matrizResultante := RemoveMatrizVazia(matriz)
	for _, item := range matrizResultante {
		tamItem := len(item)
		fmt.Println("Tamanho Matriz: ", tam)
		fmt.Println("Tamanho do Item ", tamItem)

		if tamItem < 1 {
			return false, true
		}
		save, erro, inputvazio := AddListToList(Hash, item, ConnectionMongoDB, DataBaseMongo, Collection)

		if inputvazio {
			fmt.Println(" O lista de inpts estao vazio(clusterResultante)")
		}

		if !save && erro {
			fmt.Println("Ocorreu um erro a salvar a lista de inputs(clusterResultante)")
			fmt.Println(" Nao foi salvo a lista de inputs(clusterResultante)")
			return false, true
		}
		tam = tam - 1
	}

	return true, false
}

func AddListToList(Hash string, Input []string, ConnectionMongoDB string, DataBaseMongo string, Collection string) (sucesso, erro, Inputvazio bool) {
	if len(Input) > 0 {
		cliente, contexto, cancel, errou := Database.Connect(ConnectionMongoDB)
		if errou != nil {

			fmt.Println()
			fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função AddListToList - {Function/Cluster.go}")
			fmt.Println()
			fmt.Println("Horario: ", time.Now())

			LimpaTxt("hashParaDeletar.txt")

			fmt.Println()
			fmt.Println(errou)
			fmt.Println()

			return false, true, false
		}

		Database.Ping(cliente, contexto)

		filter := bson.M{
			"hash": Hash,
		}
		update := bson.M{"$push": bson.M{"input": bson.M{"$each": Input}}}

		result, err := Database.UpdateOne(cliente, contexto, DataBaseMongo, Collection, filter, update)

		// handle the error
		if err != nil {

			fmt.Println()
			fmt.Println("Erro na resposta da função UpdateOne - {Database/Mongo.go} que esta sendo chamada na Função AddListToList - {Function/Cluster.go}")
			fmt.Println()
			fmt.Println("Horario: ", time.Now())

			LimpaTxt("hashParaDeletar.txt")

			fmt.Println()
			fmt.Println(err.Error())
			return false, true, false
		}
		defer Database.Close(cliente, contexto, cancel)

		if result.ModifiedCount > 0 {
			return true, false, false
		} else {
			return false, true, false
		}
	} else {
		LimpaTxt("hashParaDeletar.txt")

		fmt.Println("O Lista de Cluster Resultante esta vazio")
		return false, false, true
	}
	return false, false, true
}

// deleta a lista de hash que estão em hashParaDeletar.txt
func DeletarListHash(valoresHashParaDeletar []string, txt, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados string) bool {

	DeleteConfirm := DeleteListCluster(valoresHashParaDeletar, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)

	if !DeleteConfirm {
		fmt.Println("Não foram deletados todos os clusters")
		return false
	} else {
		fmt.Println("Deletado com sucesso a lista de hash em ", txt)
		fmt.Println("limpa o txt: ", txt)

		LimpaTxt("hashParaDeletar.txt")

		return true
	}
}

func DeleteListCluster(clusters []string, ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) bool {
	for _, item := range clusters {
		fmt.Println("Deletando o cluster: ", item)

		confirm := DeleteCluster(item, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)

		if !confirm {
			fmt.Println("Não foi deletado o cluster: ", item)
			return false
		}
		fmt.Println("Deletado com sucesso o cluster: ", item)
	}
	fmt.Println("Todos os clusters foram deletados")
	return true
}

func CheckCluster(ConnectionMongoDB, dataBase, col, key, code string) bool {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função CheckCluster - {Function/Cluster.go}")
		fmt.Println()
		panic(err)
	}

	// Free the resource when mainn dunction is  returned
	defer Database.Close(client, ctx, cancel)
	count, err := Database.CountElemento(client, ctx, dataBase, col, key, code)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função CountElemento - {Database/Mongo.go} que esta sendo chamada na Função CheckCluster - {Function/Cluster.go}")
		fmt.Println()
		panic(err)
	}
	if count > 0 {
		return true
	} else {
		return false
	}
}

func SaveClusterMongo(Cluster Model.Cluster, ConnectionMongoDB string, DataBaseMongo string, Collection string) (salvo bool, existente bool) {
	confirm := CheckCluster(ConnectionMongoDB, DataBaseMongo, Collection, "hash", Cluster.Hash)
	if confirm {
		fmt.Println("Esse cluster ja existe nessa Collection: ", Collection)
		return false, true
	}

	return SaveCluster(Cluster, ConnectionMongoDB, DataBaseMongo, Collection), false
}

func DeleteClusterMongo(hash string, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	confirm := CheckCluster(ConnectionMongoDB, DataBaseMongo, Collection, "hash", hash)
	if !confirm {
		fmt.Println("Esse hash não existe nessa Collection, por isso não tem como excluir: ", Collection)
		return false
	}
	return DeleteCluster(hash, ConnectionMongoDB, DataBaseMongo, Collection)
}

func PutHash(Hash string, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	if len(Hash) > 0 {
		cliente, contexto, cancel, errou := Database.Connect(ConnectionMongoDB)
		if errou != nil {

			fmt.Println()
			fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função PutCluster - {Function/Cluster.go}")
			fmt.Println()

			fmt.Println()
			fmt.Println(errou.Error())
			fmt.Println()

			return false
		}

		Database.Ping(cliente, contexto)
		defer Database.Close(cliente, contexto, cancel)

		var result *mongo.UpdateResult
		var err error

		filter := bson.M{
			"hash": Hash,
		}
		update := bson.M{"$set": bson.M{"input": nil}}

		result, err = Database.UpdateOne(cliente, contexto, DataBaseMongo, Collection, filter, update)

		// handle the error
		if err != nil {

			fmt.Println()
			fmt.Println("Erro na resposta da função UpdateOne - {Database/Mongo.go} que esta sendo chamada na Função PutCluster - {Function/Cluster.go}")
			fmt.Println()

			fmt.Println()
			fmt.Println(errou.Error())
			fmt.Println()
			return false
		}

		if result.ModifiedCount > 0 {
			return true
		} else {
			return false
		}
	} else {
		fmt.Println("O Hash do Cluster esta vazio")
		return false
	}
	return false
}

func RemoveClusterComValorDefinido(remove bool, valorDefinido int, clusters []Model.Cluster) []Model.Cluster {
	if clusters == nil {
		return clusters
	} else if len(clusters) == 0 {
		return clusters
	} else if !remove {
		return clusters
	} else {
		result := []Model.Cluster{}

		for _, item := range clusters {
			if !(len(item.Input) > valorDefinido) {
				result = append(result, item)
			}
		}
		return result
	}
}
