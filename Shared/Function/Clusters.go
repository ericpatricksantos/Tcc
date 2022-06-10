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

func GetAllMapClusterLimitOffset(limit int64, offset int64, ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (Clusters []Model.MapCluster) {
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

func SaveMapClusters(Clusters []interface{}, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	if len(Clusters) > 0 {
		cliente, contexto, cancel, errou := Database.Connect(ConnectionMongoDB)
		if errou != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go}  que esta sendo chamada na Função SaveAddrSimplificado - {Function/Clusters.go}")
			fmt.Println()

			panic(errou)
		}

		Database.Ping(cliente, contexto)
		defer Database.Close(cliente, contexto, cancel)

		//documents := []interface{}{}
		//for _, item := range Clusters {
		//	documents = append(documents, item)
		//}

		result, err := Database.InsertMany(cliente, contexto, DataBaseMongo, Collection, Clusters)
		// handle the error
		if err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função InsertMany - {Database/Mongo.go}  que esta sendo chamada na Função MapCluster - {Function/Clusters.go}")
			fmt.Println()

			panic(err)
		}

		if result.InsertedIDs != nil {
			return true
		} else {
			return false
		}

	} else {
		fmt.Println("A lista esta vazio")
		return false
	}
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

func GetAllMapClustersLimitOffset(limit, offset int64, ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (Clusters []Model.MapCluster) {
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

func AllSearchAddrMapClusters(limit int64, addr, identificadorAtual, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados string) []Model.MapCluster {
	offset := 0
	result := []Model.MapCluster{}
	for {
		clusters := SearchAddrMapClustersLimitOffset(limit, int64(offset), addr, identificadorAtual, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)
		tam := len(clusters)
		if tam == 0 {
			break
		}
		result = append(result, clusters...)
		offset = offset + tam
	}
	return result
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

func SearchAddrMapClustersLimitOffset(limit, offset int64, addr string, identificadorAtual, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados string) (result []Model.MapCluster) {
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

	cursor, err := Database.QueryLimitOffset(client, ctx, DataBaseMongo, CollectionRecuperaDados, limit, offset, filter, option)

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

func DeleteListIdentificadoresCluster(identificador []string, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
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
		"identificador": bson.M{"$in": identificador},
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

func DeleteIdentificadorCluster(identificador string, ConnectionMongoDB, DataBaseMongo, Collection string) bool {
	existe := CheckItem(ConnectionMongoDB, DataBaseMongo, Collection, "identificador", identificador)
	if !existe {
		fmt.Println(" Esse elemento nao existe: ", identificador)
		return false
	}
	return DeleteIdentificadoresCluster(identificador, ConnectionMongoDB, DataBaseMongo, Collection)
}

func CheckItem(ConnectionMongoDB, dataBase, col, key, code string) bool {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		panic(err)
	}

	// Free the resource when mainn dunction is  returned
	defer Database.Close(client, ctx, cancel)
	count, err := Database.CountElemento(client, ctx, dataBase, col, key, code)
	if err != nil {
		panic(err)
	}
	if count > 0 {
		return true
	} else {
		return false
	}
}

func DeleleListIdentificadoresCluster(identificadores []string, ConnectionMongoDB, DataBaseMongo, Collection string) (sucesso bool) {
	for _, elem := range identificadores {
		confirm := DeleteIdentificadorCluster(elem, ConnectionMongoDB, DataBaseMongo, Collection)
		if !confirm {
			fmt.Println(" Erro: Não foi Deletado os identificadores")
			return false
		}
	}
	fmt.Println(" Deletado os identificadores com Sucesso")
	return true
}

func DeleteListIdentificadoresAndClusters(identificadores []string, ConnectionMongoDB, DataBaseMongo, Collection_Identificadores, Collection_Map_Clusters string) (sucesso bool) {

	sucess := DeleteListIdentificadoresCluster(identificadores, ConnectionMongoDB, DataBaseMongo, Collection_Identificadores)
	if sucess {
		sucess = DeleteListIdentificadoresCluster(identificadores, ConnectionMongoDB, DataBaseMongo, Collection_Map_Clusters)
		if !sucess {
			fmt.Println(" Erro: Não foi Deletado os Clusters")
			return false
		}
	} else {
		fmt.Println(" Erro: Não foi Deletado os identificadores")
		return false
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

func PutIdentificadores(identificadorBase string, identificadorModificados []string, ConnectionMongoDB, DataBaseMongo, Collection string) bool {

	cliente, contexto, cancel, errou := Database.Connect(ConnectionMongoDB)
	if errou != nil {
		return false
	}

	Database.Ping(cliente, contexto)
	defer Database.Close(cliente, contexto, cancel)

	var result *mongo.UpdateResult
	var err error

	filter := bson.M{
		"identificador": bson.M{"$in": identificadorModificados},
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
	existe := CheckItem(ConnectionMongoDB, DataBaseMongo, Collection, "identificador", identificador)
	if !existe {
		fmt.Println(" Nao existe o cluster com o identificador: ", identificador)
		return false
	}
	return PutMapClusterResultante(clusterResultante, identificador, ConnectionMongoDB, DataBaseMongo, Collection)
}

func PutMapClusterResultante(clusterResultante map[string]string, identificador, ConnectionMongoDB, DataBaseMongo, Collection string) bool {
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

func TransfereClustersD1_D2(ConnectionMongoDB, DB1, DB2, CollectionI1, CollectionD1, CollectionI2, CollectionD2, DBR, CollectionIR, CollectionCR string) bool {
	var limit int64 = 2000
	var offset int64 = 0

	// Busca Clusters da Distancia 1
	for {
		clusters := GetAllClusterLimitOffsetToDocs(limit, offset, ConnectionMongoDB, DB1, CollectionD1)
		identificadores := GetAllIdentificadoresLimitOffsetToDocs(limit, offset, ConnectionMongoDB, DB1, CollectionI1)

		if len(clusters) == 0 || len(identificadores) == 0 {
			offset = 0
			break
		}

		conf1 := SaveMapClusters(clusters, ConnectionMongoDB, DBR, CollectionCR)
		conf2 := SaveMapClusters(identificadores, ConnectionMongoDB, DBR, CollectionIR)
		if !conf1 || !conf2 {
			fmt.Println("Erro - 1º For")
			return false
		}

		offset = offset + int64(len(clusters))
	}

	// Busca Clusters da Distancia 2
	for {
		clusters := GetAllClusterLimitOffsetToDocs(limit, offset, ConnectionMongoDB, DB2, CollectionD2)
		identificadores := GetAllIdentificadoresLimitOffsetToDocs(limit, offset, ConnectionMongoDB, DB2, CollectionI2)

		if len(clusters) == 0 || len(identificadores) == 0 {
			offset = 0
			break
		}

		conf1 := SaveMapClusters(clusters, ConnectionMongoDB, DBR, CollectionCR)
		conf2 := SaveMapClusters(identificadores, ConnectionMongoDB, DBR, CollectionIR)
		if !conf1 || !conf2 {
			fmt.Println("Erro - 2º For")
			return false
		}

		offset = offset + int64(len(clusters))
	}

	return true
}

func TransfereClusters(ConnectionMongoDB, DB1, CollectionI1, CollectionD1, DBR, CollectionIR, CollectionCR string) bool {
	var limit int64 = 2000
	var offset int64 = 0

	for {
		clusters := GetAllClusterLimitOffsetToDocs(limit, offset, ConnectionMongoDB, DB1, CollectionD1)

		if len(clusters) == 0 {
			offset = 0
			break
		}

		conf1 := SaveMapClusters(clusters, ConnectionMongoDB, DBR, CollectionCR)
		if !conf1 {
			fmt.Println("Erro - 1º")
			return false
		}
		offset = offset + int64(len(clusters))
	}

	for {
		identificadores := GetAllIdentificadoresLimitOffsetToDocs(limit, offset, ConnectionMongoDB, DB1, CollectionI1)

		if len(identificadores) == 0 {
			offset = 0
			break
		}

		conf2 := SaveMapClusters(identificadores, ConnectionMongoDB, DBR, CollectionIR)

		if !conf2 {
			fmt.Println("Erro - 2º")
			return false
		}

		offset = offset + int64(len(identificadores))
	}

	return true
}

func ContagemIdentificadoresClusters(ConnectionMongoDB, DB1, CollectionCluster string) map[string]string {
	var limit int64 = 2000
	var offset int64 = 0
	result := map[string]string{}
	for {
		clusters := GetAllMapClustersLimitOffset(limit, offset, ConnectionMongoDB, DB1, CollectionCluster)

		if len(clusters) == 0 {
			offset = 0
			break
		}
		for _, cluster := range clusters {
			_, ok := result[cluster.Identificador]
			if ok {
				continue
			} else {
				result[cluster.Identificador] = ""
			}
		}

		offset = offset + int64(len(clusters))
	}
	return result
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

func GetAllIdentificadores(limit, offset int64, ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (Identificadors []Model.Identificador) {
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
		var identificador Model.Identificador

		if err := cursor.Decode(&identificador); err != nil {
			log.Fatal(err)
		}
		Identificadors = append(Identificadors, identificador)
	}
	return Identificadors
}

func UnionIdentificadores(identificador_1, identificador_2 []Model.Identificador) map[string]string {
	result := map[string]string{}

	for _, item := range identificador_1 {
		_, ok := result[item.Identificador]
		if !ok {
			result[item.Identificador] = ""
		}
	}
	for _, item := range identificador_2 {
		_, ok := result[item.Identificador]
		if !ok {
			result[item.Identificador] = ""
		}
	}

	return result
}

func UnionIdentificadoresD1_D2(ConnectionMongoDB, DataBaseMongo, CollectionD1, CollectionD2 string) map[string]string {
	result := map[string]string{}
	var limit int64 = 100000
	var offset int64 = 0

	// Busca Transações da Distancia 1
	for {
		tx_d1 := GetAllIdentificadores(limit, offset, ConnectionMongoDB, DataBaseMongo, CollectionD1)

		if len(tx_d1) == 0 {
			offset = 0
			break
		}

		for _, item := range tx_d1 {
			_, ok := result[item.Identificador]
			if !ok {
				result[item.Identificador] = ""
			}
		}

		offset = offset + int64(len(tx_d1))
	}

	// Busca Transações da Distancia 2
	for {
		tx_d2 := GetAllIdentificadores(limit, offset, ConnectionMongoDB, DataBaseMongo, CollectionD2)

		if len(tx_d2) == 0 {
			offset = 0
			break
		}

		for _, item := range tx_d2 {
			_, ok := result[item.Identificador]
			if !ok {
				result[item.Identificador] = ""
			}
		}

		offset = offset + int64(len(tx_d2))
	}

	return result
}

func SaveIdentificadores(identificadores []interface{}, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	if len(identificadores) > 0 {
		cliente, contexto, cancel, errou := Database.Connect(ConnectionMongoDB)
		if errou != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go}  que esta sendo chamada na Função SaveAddrSimplificado - {Function/identificadores.go}")
			fmt.Println()

			panic(errou)
		}

		Database.Ping(cliente, contexto)
		defer Database.Close(cliente, contexto, cancel)

		//documents := []interface{}{}
		//for _, item := range identificadores {
		//	documents = append(documents, item)
		//}

		result, err := Database.InsertMany(cliente, contexto, DataBaseMongo, Collection, identificadores)
		// handle the error
		if err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função InsertMany - {Database/Mongo.go}  que esta sendo chamada na Função SaveIdentificadores - {Function/identificadores.go}")
			fmt.Println()

			panic(err)
		}

		if result.InsertedIDs != nil {
			return true
		} else {
			return false
		}

	} else {
		fmt.Println("A lista esta vazio")
		return false
	}
}
