package Function

import (
	"Tcc/Shared/API"
	"Tcc/Shared/Database"
	"Tcc/Shared/Model"
	"fmt"
	"log"

	"gopkg.in/mgo.v2/bson"
)

// Funções para Addr

func SaveAddr(Addr Model.UnicoEndereco, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	if len(Addr.Address) > 0 {
		cliente, contexto, cancel, errou := Database.Connect(ConnectionMongoDB)
		if errou != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go}  que esta sendo chamada na Função SaveAddr - {Function/Addr.go}")
			fmt.Println()

			panic(errou)
		}

		Database.Ping(cliente, contexto)
		defer Database.Close(cliente, contexto, cancel)

		Database.ToDoc(Addr)

		result, err := Database.InsertOne(cliente, contexto, DataBaseMongo, Collection, Addr)
		// handle the error
		if err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função InsertOne - {Database/Mongo.go}  que esta sendo chamada na Função SaveAddr - {Function/Addr.go}")
			fmt.Println()

			panic(err)
		}

		if result.InsertedID != nil {
			return true
		} else {
			return false
		}

	} else {
		fmt.Println("Addr esta vazio -", Addr.Address, "-")
		return false
	}
}

func SaveAddrSimplificado(Addr Model.Endereco, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	if len(Addr.Address) > 0 {
		cliente, contexto, cancel, errou := Database.Connect(ConnectionMongoDB)
		if errou != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go}  que esta sendo chamada na Função SaveAddrSimplificado - {Function/Addr.go}")
			fmt.Println()

			panic(errou)
		}

		Database.Ping(cliente, contexto)
		defer Database.Close(cliente, contexto, cancel)

		Database.ToDoc(Addr)

		result, err := Database.InsertOne(cliente, contexto, DataBaseMongo, Collection, Addr)
		// handle the error
		if err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função InsertOne - {Database/Mongo.go}  que esta sendo chamada na Função SaveAddrSimplificado - {Function/Addr.go}")
			fmt.Println()

			panic(err)
		}

		if result.InsertedID != nil {
			return true
		} else {
			return false
		}

	} else {
		fmt.Println("Addr esta vazio -", Addr.Address, "-")
		return false
	}
}

func SaveAddrSimplificadoEmPartes(Addr Model.Endereco, limit int, ConnectionMongoDB, DataBaseMongo, Collection string) bool {
	if len(Addr.Address) > 0 && len(Addr.Txs) > 0 {
		if len(Addr.Txs) <= limit {
			return SaveAddrSimplificado(Addr, ConnectionMongoDB, DataBaseMongo, Collection)
		} else {
			var save_ate_2000 bool
			var save_ate_4000 bool
			var save_ate_5000 bool
			tamanhoTxs := len(Addr.Txs)
			ate_2000 := Model.Endereco{
				Hash160: Addr.Hash160,
				Address: Addr.Address,
				N_tx:    Addr.N_tx,
				Txs:     []Model.Tx{},
			}
			ate_4000 := Model.Endereco{
				Hash160: Addr.Hash160,
				Address: Addr.Address,
				N_tx:    Addr.N_tx,
				Txs:     []Model.Tx{},
			}
			ate_5000 := Model.Endereco{
				Hash160: Addr.Hash160,
				Address: Addr.Address,
				N_tx:    Addr.N_tx,
				Txs:     []Model.Tx{},
			}
			if tamanhoTxs <= 4000 {
				for i := 0; i < 2000; i++ {
					ate_2000.Txs = append(ate_2000.Txs, Addr.Txs[i])
				}
				for i := 2000; i < tamanhoTxs; i++ {
					ate_4000.Txs = append(ate_4000.Txs, Addr.Txs[i])
				}

				tamanhoTxsSendoSalvas := len(ate_2000.Txs) + len(ate_4000.Txs)
				if tamanhoTxsSendoSalvas != tamanhoTxs {
					fmt.Println("Tamanho Txs: ", tamanhoTxs)
					fmt.Println("Tamanho Txs sendo Salvas: ", tamanhoTxsSendoSalvas)
					return false
				}

				save_ate_2000 = SaveAddrSimplificado(ate_2000, ConnectionMongoDB, DataBaseMongo, Collection)
				save_ate_4000 = SaveAddrSimplificado(ate_4000, ConnectionMongoDB, DataBaseMongo, Collection)
				if save_ate_2000 && save_ate_4000 {
					return true
				} else {
					return false
				}
			} else if tamanhoTxs <= 5000 {
				for i := 0; i < 2000; i++ {
					ate_2000.Txs = append(ate_2000.Txs, Addr.Txs[i])
				}
				for k := 2000; k < 4000; k++ {
					ate_4000.Txs = append(ate_4000.Txs, Addr.Txs[k])
				}

				for j := 4000; j < tamanhoTxs; j++ {
					ate_5000.Txs = append(ate_5000.Txs, Addr.Txs[j])
				}
				tamanhoTxsSendoSalvas := len(ate_2000.Txs) + len(ate_4000.Txs) + len(ate_5000.Txs)
				if tamanhoTxsSendoSalvas != tamanhoTxs {
					fmt.Println("Tamanho Txs: ", tamanhoTxs)
					fmt.Println("Tamanho Txs sendo Salvas: ", tamanhoTxsSendoSalvas)
					return false
				}

				save_ate_2000 = SaveAddrSimplificado(ate_2000, ConnectionMongoDB, DataBaseMongo, Collection)
				save_ate_4000 = SaveAddrSimplificado(ate_4000, ConnectionMongoDB, DataBaseMongo, Collection)
				save_ate_5000 = SaveAddrSimplificado(ate_5000, ConnectionMongoDB, DataBaseMongo, Collection)

				if save_ate_2000 && save_ate_4000 && save_ate_5000 {
					return true
				} else {
					return false
				}
			}
			fmt.Println(" Txs é maior que 5000. Tamanho da txs: ", tamanhoTxs)
			return false
		}
	} else {
		return false
	}
}

func SaveAddrSimplificadoList(Addr []Model.Endereco, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	if len(Addr) > 0 {
		cliente, contexto, cancel, errou := Database.Connect(ConnectionMongoDB)
		if errou != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go}  que esta sendo chamada na Função SaveAddrSimplificado - {Function/Addr.go}")
			fmt.Println()

			panic(errou)
		}

		Database.Ping(cliente, contexto)
		defer Database.Close(cliente, contexto, cancel)

		documents := []interface{}{}
		for _, item := range Addr {
			documents = append(documents, item)
		}

		result, err := Database.InsertMany(cliente, contexto, DataBaseMongo, Collection, documents)
		// handle the error
		if err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função InsertOne - {Database/Mongo.go}  que esta sendo chamada na Função SaveAddrSimplificado - {Function/Addr.go}")
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

func SalveAddrSimplicadoMongoDB(addr Model.Endereco, ConnectionMongoDB, DataBaseMongo, Collection string) bool {
	confirm := CheckAddr(ConnectionMongoDB, DataBaseMongo, Collection, "address", addr.Address)
	if confirm {
		fmt.Println("Esse addr ja existe nessa Collection: ", Collection)
		return false
	}
	return SaveAddrSimplificado(addr, ConnectionMongoDB, DataBaseMongo, Collection)
}

func SalveAddrMongoDB(addr Model.UnicoEndereco, ConnectionMongoDB, DataBaseMongo, Collection string) bool {
	confirm := CheckAddr(ConnectionMongoDB, DataBaseMongo, Collection, "address", addr.Address)
	if confirm {
		fmt.Println("Esse addr ja existe nessa Collection: ", Collection)
		return false
	}
	return SaveAddr(addr, ConnectionMongoDB, DataBaseMongo, Collection)
}

func SaveAddrMongoDB(addr Model.UnicoEndereco, ConnectionMongoDB, DataBaseMongo, Collection string) (saveSucess bool, existe bool) {
	confirm := CheckAddr(ConnectionMongoDB, DataBaseMongo, Collection, "address", addr.Address)
	if confirm {
		fmt.Println("Esse addr ja existe nessa Collection: ", Collection)
		return false, true
	}
	return SaveAddr(addr, ConnectionMongoDB, DataBaseMongo, Collection), false
}

func GetAddr(endereco, urlAPI, RawAddr string) Model.UnicoEndereco {
	return API.GetUnicoEndereco(endereco, urlAPI, RawAddr)
}

func GetEndereco(endereco, urlAPI, RawAddr string, limit, offset int) Model.Endereco {
	return API.GetEndereco(endereco, urlAPI, RawAddr, limit, offset)
}

func GetAddrMongoDB(ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (addr Model.UnicoEndereco) {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go}  que esta sendo chamada na Função GetAddrMongoDB - {Function/Addr.go}")
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
		fmt.Println("Erro na resposta da função Query - {Database/Mongo.go}  que esta sendo chamada na Função GetAddrMongoDB - {Function/Addr.go}")
		fmt.Println()

		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {

		if err := cursor.Decode(&addr); err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Decode que esta sendo chamada na Função GetAddrMongoDB - {Function/Addr.go}")
			fmt.Println()

			log.Fatal(err)
		}

		return addr
	}

	return Model.UnicoEndereco{}
}

func CheckAddr(ConnectionMongoDB, dataBase, col, key, code string) bool {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função CheckAddr - {Function/Addr.go}")
		fmt.Println()
		panic(err)
	}

	// Free the resource when mainn dunction is  returned
	defer Database.Close(client, ctx, cancel)
	count, err := Database.CountElemento(client, ctx, dataBase, col, key, code)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função CountElemento - {Database/Mongo.go} que esta sendo chamada na Função CheckAddr - {Function/Addr.go}")
		fmt.Println()
		panic(err)
	}
	if count > 0 {
		return true
	} else {
		return false
	}
}

func DeleteAddrMongo(addr string, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	confirm := CheckAddr(ConnectionMongoDB, DataBaseMongo, Collection, "address", addr)
	if !confirm {
		fmt.Println("Esse Addr não existe nessa Collection, por isso não tem como excluir: ", Collection)
		return false
	}
	return DeleteAddr(addr, ConnectionMongoDB, DataBaseMongo, Collection)
}

func DeleteAddr(addr string, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função DeleteAddr - {Function/Addr.go}")
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
		"address": addr,
	}

	cursor, err := Database.DeleteOne(client, ctx, DataBaseMongo, Collection, filter)

	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função DeleteOne - {Database/Mongo.go} que esta sendo chamada na Função DeleteAddr - {Function/Addr.go}")
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

func GetAllAddr(ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (addrs []Model.UnicoEndereco) {

	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go}  que esta sendo chamada na Função GetAllAddr - {Function/Addr.go}")
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
		fmt.Println("Erro na resposta da função Query - {Database/Mongo.go}  que esta sendo chamada na Função GetAllAddr - {Function/Addr.go}")
		fmt.Println()

		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var addr Model.UnicoEndereco

		if err := cursor.Decode(&addr); err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Decode que esta sendo chamada na Função GetAllAddr - {Function/Addr.go}")
			fmt.Println()

			log.Fatal(err)
		}

		addrs = append(addrs, addr)

	}

	return addrs
}

func GetAllAddrLimit(limit int64, ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (addrs []Model.Endereco) {

	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go}  que esta sendo chamada na Função GetAllAddr - {Function/Addr.go}")
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
	cursor, err := Database.QueryLimit(client, ctx, DataBaseMongo,
		CollectionRecuperaDados, limit, filter, option)
	// handle the errors.
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Query - {Database/Mongo.go}  que esta sendo chamada na Função GetAllAddr - {Function/Addr.go}")
		fmt.Println()

		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var addr Model.Endereco

		if err := cursor.Decode(&addr); err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Decode que esta sendo chamada na Função GetAllAddr - {Function/Addr.go}")
			fmt.Println()

			log.Fatal(err)
		}

		addrs = append(addrs, addr)

	}

	return addrs
}

func GetAllAddrLimitOffset(limit, offset int64, ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (addrs []Model.Endereco) {

	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go}  que esta sendo chamada na Função GetAllAddr - {Function/Addr.go}")
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
	cursor, err := Database.QueryLimitOffset(client, ctx, DataBaseMongo,
		CollectionRecuperaDados, limit, offset, filter, option)
	// handle the errors.
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Query - {Database/Mongo.go}  que esta sendo chamada na Função GetAllAddr - {Function/Addr.go}")
		fmt.Println()

		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var addr Model.Endereco

		if err := cursor.Decode(&addr); err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Decode que esta sendo chamada na Função GetAllAddr - {Function/Addr.go}")
			fmt.Println()

			log.Fatal(err)
		}

		addrs = append(addrs, addr)

	}

	return addrs
}

func UnionHashTransacaoD1_D2(ConnectionMongoDB, DataBaseMongo, CollectionD1, CollectionD2 string) map[string]string {
	result := map[string]string{}
	var limit int64 = 1000
	var offset int64 = 0

	// Busca Transações da Distancia 1
	for {
		enderecos := GetAllAddrLimitOffset(limit, offset, ConnectionMongoDB, DataBaseMongo, CollectionD1)

		if len(enderecos) == 0 {
			offset = 0
			break
		}
		for _, endereco := range enderecos {
			for _, tx := range endereco.Txs {
				_, ok := result[tx.Hash]
				if !ok {
					result[tx.Hash] = ""
				}
			}
		}

		offset = offset + int64(len(enderecos))
	}

	fmt.Println(len(result))
	// Busca Transações da Distancia 2
	for {
		enderecos := GetAllAddrLimitOffset(limit, offset, ConnectionMongoDB, DataBaseMongo, CollectionD2)

		if len(enderecos) == 0 {
			offset = 0
			break
		}

		for _, endereco := range enderecos {
			for _, tx := range endereco.Txs {
				_, ok := result[tx.Hash]
				if !ok {
					result[tx.Hash] = ""
				}
			}
		}

		offset = offset + int64(len(enderecos))
	}

	return result
}

func UnionEnderecosD1_D2(ConnectionMongoDB, DataBaseMongo, CollectionD1, CollectionD2 string) map[string]string {
	result := map[string]string{}
	var limit int64 = 1000
	var offset int64 = 0

	// Busca Transações da Distancia 1
	for {
		enderecos := GetAllAddrLimitOffset(limit, offset, ConnectionMongoDB, DataBaseMongo, CollectionD1)

		if len(enderecos) == 0 {
			offset = 0
			break
		}
		for _, endereco := range enderecos {
			for _, tx := range endereco.Txs {
				for _, addr := range tx.Inputs {
					_, ok := result[addr.Prev_Out.Addr]
					if !ok {
						result[addr.Prev_Out.Addr] = ""
					}
				}
			}
		}

		offset = offset + int64(len(enderecos))
	}

	fmt.Println(len(result))
	// Busca Transações da Distancia 2
	for {
		enderecos := GetAllAddrLimitOffset(limit, offset, ConnectionMongoDB, DataBaseMongo, CollectionD2)

		if len(enderecos) == 0 {
			offset = 0
			break
		}

		for _, endereco := range enderecos {
			for _, tx := range endereco.Txs {
				for _, addr := range tx.Inputs {
					_, ok := result[addr.Prev_Out.Addr]
					if !ok {
						result[addr.Prev_Out.Addr] = ""
					}
				}
			}
		}

		offset = offset + int64(len(enderecos))
	}

	return result
}

func ContagemEnderecosTotais(ConnectionMongoDB, DataBaseMongo, CollectionD1, CollectionD2 string) map[string]string {
	result := map[string]string{}
	var limit int64 = 100000
	var offset int64 = 0

	// Busca Transações da Distancia 1
	for {
		tx_d1 := GetAllMapClusterLimitOffset(limit, offset, ConnectionMongoDB, DataBaseMongo, CollectionD1)

		if len(tx_d1) == 0 {
			offset = 0
			break
		}

		for _, item := range tx_d1 {
			for ch, _ := range item.Clusters {
				_, ok := result[ch]
				if !ok {
					result[ch] = ""
				}
			}
		}

		offset = offset + int64(len(tx_d1))
	}

	fmt.Println(len(result))
	// Busca Transações da Distancia 2
	for {
		tx_d1 := GetAllMapClusterLimitOffset(limit, offset, ConnectionMongoDB, DataBaseMongo, CollectionD2)

		if len(tx_d1) == 0 {
			offset = 0
			break
		}

		for _, item := range tx_d1 {
			for ch, _ := range item.Clusters {
				_, ok := result[ch]
				if !ok {
					result[ch] = ""
				}
			}
		}

		offset = offset + int64(len(tx_d1))
	}

	return result
}

func ContagemEnderecosClusters(ConnectionMongoDB, DataBaseMongo, Collection string) map[string]string {
	result := map[string]string{}
	var limit int64 = 2000
	var offset int64 = 0

	for {
		tx_d1 := GetAllMapClusterLimitOffset(limit, offset, ConnectionMongoDB, DataBaseMongo, Collection)

		if len(tx_d1) == 0 {
			offset = 0
			break
		}

		for _, item := range tx_d1 {
			for ch, _ := range item.Clusters {
				_, ok := result[ch]
				if !ok {
					result[ch] = ""
				}
			}
		}

		offset = offset + int64(len(tx_d1))
	}

	return result
}

func GetAddrsByAddress(addr, ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (addrs []Model.Endereco) {

	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go}  que esta sendo chamada na Função GetAllAddr - {Function/Addr.go}")
		fmt.Println()

		panic(err)
	}

	// Free the resource when mainn dunction is  returned
	defer Database.Close(client, ctx, cancel)

	var filter, option interface{}

	filter = bson.M{
		"address": addr,
	}

	//  option remove id field from all documents
	option = bson.M{}

	cursor, err := Database.QueryLimit(client, ctx, DataBaseMongo,
		CollectionRecuperaDados, 10000000000000000, filter, option)
	// handle the errors.
	if err != nil {
		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var addr Model.Endereco

		if err := cursor.Decode(&addr); err != nil {
			log.Fatal(err)
		}

		addrs = append(addrs, addr)

	}

	return addrs
}

func SearchAddr(addr string, ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (result []Model.UnicoEndereco) {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função SearchAddr - {Function/Cluster.go}")
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

	cursor, err := Database.Query(client, ctx, DataBaseMongo, CollectionRecuperaDados, filter, option)

	// handle the errors.
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função Query - {Database/Mongo.go} que esta sendo chamada na Função SearchAddr - {Function/Cluster.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var addr Model.UnicoEndereco

		if err := cursor.Decode(&addr); err != nil {

			fmt.Println()
			fmt.Println("Erro na resposta da função Decode que esta sendo chamada na Função SearchAddr - {Function/Cluster.go}")
			fmt.Println()

			fmt.Println()
			fmt.Println(err.Error())
			fmt.Println()

			log.Fatal(err)
		}

		result = append(result, addr)

	}

	return result
}

// AP - AwaitingProcessing
// P  - Processing
func MudancaStatusAddr_AP_P(addr Model.UnicoEndereco, ConnectionMongoDB, DataBase, awaitingProcessing, processing string) bool {

	salvo := SalveAddrMongoDB(addr, ConnectionMongoDB, DataBase, processing)

	if !salvo {
		fmt.Println("Não foi salvo com Sucesso")
		return false
	} else {
		fmt.Println("Salvo com sucesso a Addr na collection ", processing)
	}

	deletado := DeleteAddrMongo(addr.Address, ConnectionMongoDB, DataBase, awaitingProcessing)

	if !deletado {
		fmt.Println("Address: ", addr.Address, " não foi deletado de ", awaitingProcessing)
		return false
	} else {
		fmt.Println("Deletado com sucesso a Addr da collection ", awaitingProcessing)
	}

	return true
}

func MudancaStatusAddr_Processing_Processed(addr Model.UnicoEndereco, ConnectionMongoDB, DataBase, processing, processed string) bool {

	salvo := SalveAddrMongoDB(addr, ConnectionMongoDB, DataBase, processed)

	if !salvo {
		fmt.Println("Não foi salvo com Sucesso")
		return false
	} else {
		fmt.Println("Salvo com sucesso a Addr na collection ", processed)
	}

	deletado := DeleteAddrMongo(addr.Address, ConnectionMongoDB, DataBase, processing)

	if !deletado {
		fmt.Println("Address: ", addr.Address, " não foi deletado de ", processing)
		return false
	} else {
		fmt.Println("Deletado com sucesso a Addr da collection ", processing)
	}

	return true
}

func MudancaStatusAddr(addr Model.UnicoEndereco, ConnectionMongoDB, DataBase, collectionOrigem, collectionDestino string) (mudou bool, existente bool) {
	salvo, existe := SaveAddrMongoDB(addr, ConnectionMongoDB, DataBase, collectionDestino)

	if !salvo && !existe {
		fmt.Println("Não foi salvo com Sucesso a Addr na collection ", collectionDestino)
		return false, false
	} else if !salvo && existente {
		fmt.Println("Essa Addr ja existente na collection ", collectionDestino)
	} else {
		fmt.Println("Salvo com sucesso a Addr na collection ", collectionDestino)
	}

	deletado := DeleteAddrMongo(addr.Address, ConnectionMongoDB, DataBase, collectionOrigem)

	if !deletado {
		fmt.Println("Addr: ", addr.Address, " não foi deletado de ", collectionOrigem)
		return false, false
	} else {
		fmt.Println("Deletado com sucesso a tx da collection", collectionOrigem)
	}

	return true, false
}

// Funções para MultiAddr

func SaveMultiAddr(MultiAddr Model.MultiEndereco, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	if len(MultiAddr.Addresses) > 0 {
		cliente, contexto, cancel, errou := Database.Connect(ConnectionMongoDB)
		if errou != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go}  que esta sendo chamada na Função SaveMultiAddr - {Function/Addr.go}")
			fmt.Println()

			panic(errou)
		}

		Database.Ping(cliente, contexto)
		defer Database.Close(cliente, contexto, cancel)

		Database.ToDoc(MultiAddr)

		result, err := Database.InsertOne(cliente, contexto, DataBaseMongo, Collection, MultiAddr)

		// handle the error
		if err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função InsertOne - {Database/Mongo.go}  que esta sendo chamada na Função SaveMultiAddr - {Function/Addr.go}")
			fmt.Println()

			panic(err)
		}

		if result.InsertedID != nil {
			return true
		} else {
			return false
		}

	} else {
		fmt.Println("Array de MultiAddr esta vazio -", len(MultiAddr.Addresses), "-")
		return false
	}
}

// resolver esse metodo
func SalveMultiAddrMongoDB(MultiAddr Model.MultiEndereco, ConnectionMongoDB, DataBaseMongo, Collection string) bool {
	//confirm := CheckMultiAddr(ConnectionMongoDB, DataBaseMongo, Collection, "address", MultiAddr.Addresses)
	//if confirm {
	//	fmt.Println("Esse MultiAddr ja existe nessa Collection: ", Collection)
	//	return false
	//}
	return SaveMultiAddr(MultiAddr, ConnectionMongoDB, DataBaseMongo, Collection)
}

// Busca na API
func GetMultiAddr(MultiAddr []string, urlAPI, endpoint string, limit, offset int) Model.MultiEndereco {
	return API.GetMultiAddr(MultiAddr, urlAPI, endpoint, limit, offset)
}

func GetAddrMultiMongoDB(ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (MultiAddr Model.MultiEndereco) {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go}  que esta sendo chamada na Função GetAddrMultiMongoDB - {Function/Addr.go}")
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
		fmt.Println("Erro na resposta da função Query - {Database/Mongo.go}  que esta sendo chamada na Função GetAddrMultiMongoDB - {Function/Addr.go}")
		fmt.Println()

		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {

		if err := cursor.Decode(&MultiAddr); err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Decode que esta sendo chamada na Função GetAddrMultiMongoDB - {Function/Addr.go}")
			fmt.Println()

			log.Fatal(err)
		}

		return MultiAddr
	}

	return Model.MultiEndereco{}
}

// resolver esse metodo
func CheckMultiAddr(ConnectionMongoDB, dataBase, col, key string, code []string) bool {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função CheckMultiAddr - {Function/Addr.go}")
		fmt.Println()
		panic(err)
	}

	// Free the resource when mainn dunction is  returned
	defer Database.Close(client, ctx, cancel)
	for _, item := range code {
		count, err := Database.CountElemento(client, ctx, dataBase, col, key, item)
		if err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função CountElemento - {Database/Mongo.go} que esta sendo chamada na Função CheckMultiAddr - {Function/Addr.go}")
			fmt.Println()
			panic(err)
		}
		if count > 0 {
			return true
		} else {
			return false
		}
	}
	return false
}

// resolver esse metodo
func DeleteMultiAddrMongo(MultiAddr string, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	//confirm := CheckMultiAddr(ConnectionMongoDB, DataBaseMongo, Collection, "address", MultiAddr)
	//if !confirm {
	//	fmt.Println("Esse Addr não existe nessa Collection, por isso não tem como excluir: ", Collection)
	//	return false
	//}
	return DeleteMultiAddr(MultiAddr, ConnectionMongoDB, DataBaseMongo, Collection)
}

// resolver esse metodo
func DeleteMultiAddr(MultiAddr string, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função DeleteMultiAddr - {Function/Addr.go}")
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
		"address": MultiAddr,
	}

	cursor, err := Database.DeleteOne(client, ctx, DataBaseMongo, Collection, filter)

	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função DeleteOne - {Database/Mongo.go} que esta sendo chamada na Função DeleteMultiAddr - {Function/Addr.go}")
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

func GetAllMultiAddr(ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (multiAddrs []Model.MultiEndereco) {

	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go}  que esta sendo chamada na Função GetAllAddr - {Function/Addr.go}")
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
		fmt.Println("Erro na resposta da função Query - {Database/Mongo.go}  que esta sendo chamada na Função GetAllAddr - {Function/Addr.go}")
		fmt.Println()

		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var multiAddr Model.MultiEndereco

		if err := cursor.Decode(&multiAddr); err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Decode que esta sendo chamada na Função GetAllAddr - {Function/Addr.go}")
			fmt.Println()

			log.Fatal(err)
		}

		multiAddrs = append(multiAddrs, multiAddr)

	}

	return multiAddrs
}

func UltimoValor(valorReal int, dividendo float32) int {
	valorRealFracionario := float32(valorReal) / dividendo
	valorRealInteiro := valorReal / int(dividendo)
	resultadoParcial := valorRealFracionario - float32(valorRealInteiro)
	resultado := int(resultadoParcial*dividendo) + 1
	return resultado
}

func DividiTransacoesDosEndereco(respostaCompleta Model.Endereco, qtdDivisoes float32) (resultado []Model.Endereco) {
	tamanho := len(respostaCompleta.Txs)
	ultimoValor := UltimoValor(tamanho, qtdDivisoes)
	if tamanho <= int(qtdDivisoes) {
		resultado = append(resultado, respostaCompleta)
	} else {
		temp := Model.Endereco{
			Hash160: respostaCompleta.Hash160,
			Address: respostaCompleta.Address,
			N_tx:    respostaCompleta.N_tx,
		}
		for indice, item := range respostaCompleta.Txs {
			temp.Txs = append(temp.Txs, item)

			if len(temp.Txs) == int(qtdDivisoes) {
				resultado = append(resultado, temp)
				temp.Txs = nil
			} else if len(temp.Txs) == ultimoValor && indice == tamanho-1 {
				resultado = append(resultado, temp)
				temp.Txs = nil
			}
		}
	}

	return resultado
}
