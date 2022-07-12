package Function

import (
	"Tcc/Shared/Database"
	"Tcc/Shared/Model"
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"log"
)

// Funções para AddrAnalisado

func SaveAddrAnalisado(addr Model.AddressAnalisado, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	if len(addr.Address) > 0 {
		cliente, contexto, cancel, errou := Database.Connect(ConnectionMongoDB)
		if errou != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go}  que esta sendo chamada na Função SaveAddrAnalisado - {Function/Distance.go}")
			fmt.Println()

			panic(errou)
		}

		Database.Ping(cliente, contexto)
		defer Database.Close(cliente, contexto, cancel)

		Database.ToDoc(addr)

		result, err := Database.InsertOne(cliente, contexto, DataBaseMongo, Collection, addr)

		// handle the error
		if err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função InsertOne - {Database/Mongo.go}  que esta sendo chamada na Função SaveAddrAnalisado - {Function/Distance.go}")
			fmt.Println()

			panic(err)
		}

		if result.InsertedID != nil {
			return true
		} else {
			return false
		}

	} else {
		fmt.Println("Addr esta vazio -", addr.Address, "-")
		return false
	}
}

func SalveAddrAnalisadoMongoDB(addr Model.AddressAnalisado, ConnectionMongoDB, DataBaseMongo, Collection string) bool {
	confirm := CheckAddrAnalisado(ConnectionMongoDB, DataBaseMongo, Collection, "address", addr.Address)
	if confirm {
		fmt.Println("Esse addr ja existe nessa Collection: ", Collection)
		return false
	}
	return SaveAddrAnalisado(addr, ConnectionMongoDB, DataBaseMongo, Collection)
}

func GetAddrAnalisadoMongoDB(ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (analise Model.AddressAnalisado) {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go}  que esta sendo chamada na Função GetAddrAnalisadoMongoDB - {Function/Distance.go}")
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
		fmt.Println("Erro na resposta da função Query - {Database/Mongo.go}  que esta sendo chamada na Função GetAddrAnalisadoMongoDB - {Function/Distance.go}")
		fmt.Println()

		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {

		if err := cursor.Decode(&analise); err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Decode que esta sendo chamada na Função GetAddrAnalisadoMongoDB - {Function/Distance.go}")
			fmt.Println()

			log.Fatal(err)
		}

		return analise
	}

	return Model.AddressAnalisado{}
}

func CheckAddrAnalisado(ConnectionMongoDB, dataBase, col, key, code string) bool {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função CheckAddrAnalisado - {Function/Distance.go}")
		fmt.Println()
		panic(err)
	}

	// Free the resource when mainn dunction is  returned
	defer Database.Close(client, ctx, cancel)
	count, err := Database.CountElemento(client, ctx, dataBase, col, key, code)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função CountElemento - {Database/Mongo.go} que esta sendo chamada na Função CheckAddrAnalisado - {Function/Distance.go}")
		fmt.Println()
		panic(err)
	}
	if count > 0 {
		return true
	} else {
		return false
	}
}

func DeleteAddrAnalisadoMongo(addr string, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	confirm := CheckAddrAnalisado(ConnectionMongoDB, DataBaseMongo, Collection, "address", addr)
	if !confirm {
		fmt.Println("Esse Addr não existe nessa Collection, por isso não tem como excluir: ", Collection)
		return false
	}
	return DeleteAddrAnalisado(addr, ConnectionMongoDB, DataBaseMongo, Collection)
}

func DeleteAddrAnalisado(analise string, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função DeleteAddrAnalisado - {Function/Distance.go}")
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
		"address": analise,
	}

	cursor, err := Database.DeleteOne(client, ctx, DataBaseMongo, Collection, filter)

	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função DeleteOne - {Database/Mongo.go} que esta sendo chamada na Função DeleteAddrAnalisado - {Function/Distance.go}")
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

func GetAllAddrAnalisado(ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (analise []Model.AddressAnalisado) {

	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go}  que esta sendo chamada na Função GetAllAddrAnalisado - {Function/Distance.go}")
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
		fmt.Println("Erro na resposta da função Query - {Database/Mongo.go}  que esta sendo chamada na Função GetAllAddrAnalisado - {Function/Distance.go}")
		fmt.Println()

		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var temp Model.AddressAnalisado

		if err := cursor.Decode(&temp); err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Decode que esta sendo chamada na Função GetAllAddrAnalisado - {Function/Addr.go}")
			fmt.Println()

			log.Fatal(err)
		}

		analise = append(analise, temp)

	}

	return analise
}

func ProcessAdressesAnalysis(ConnectionMongoDB, DataBaseAnalise, CollectionAnaliseAwaitingProcessing, CollectionAnaliseProcessed, DataBaseAddr, CollectionAddr, urlAPI, RawAddr string) bool {
	fmt.Println("Buscando todos os Endereços que serão analisados")
	addrAnalisados := GetAllAddrAnalisado(ConnectionMongoDB, DataBaseAnalise, CollectionAnaliseAwaitingProcessing)

	for _, item := range addrAnalisados {
		fmt.Println("Buscando na API o endereço ", item.Address)
		addr := GetAddr(item.Address, urlAPI, RawAddr)

		if len(addr.Address) > 0 && len(addr.Txs) > 0 && len(addr.Txs) == addr.N_tx {
			fmt.Println("Salvando o Endereco no MongoDB ")
			confirmSalve := SalveAddrMongoDB(addr, ConnectionMongoDB, DataBaseAddr, CollectionAddr)
			if confirmSalve {
				fmt.Println("Endereco Salvo ", item.Address)
				fmt.Println("Mudança de Status awaitingProcessing -> Processed")
				confirm := SalveAddrAnalisadoMongoDB(item, ConnectionMongoDB, DataBaseAnalise, CollectionAnaliseProcessed)
				if confirm {
					fmt.Println("Salvo com Sucesso")
					fmt.Println("Deletando endereco da Collection ", CollectionAnaliseAwaitingProcessing)
					confirmDelete := DeleteAddrAnalisadoMongo(item.Address, ConnectionMongoDB, DataBaseAnalise, CollectionAnaliseAwaitingProcessing)
					if confirmDelete {
						fmt.Println("Deletado com Sucesso ")
						return true
					} else {
						fmt.Println("Falha ao deletar ", item.Address, " da Collection ", CollectionAnaliseAwaitingProcessing)
						return false
					}
				} else {
					fmt.Println("Não foi Salvo")
					return false
				}

			} else {
				fmt.Println("Falha ao salva endereco ", item.Address)
				return false
			}
		}
	}

	return false
}
func ToAddrAnalisados(allAddrs []Model.AddressAnalisado, addrsBaixados []Model.Endereco) map[string]string {
	addrsBaixadosMap := map[string]string{}
	result := map[string]string{}
	for _, item := range addrsBaixados {
		addrsBaixadosMap[item.Address] = item.Address
	}
	for _, item := range allAddrs {
		_, ok := addrsBaixadosMap[item.Address]
		if ok {
			continue
		} else {
			result[item.Address] = item.Address
		}
	}
	return result
}
func ProcessAdressesAnalysis_v2(ConnectionMongoDB, DataBaseAnalise, CollectionAnaliseAwaitingProcessing, DataBaseAddr, CollectionAddr, urlAPI, RawAddr string) bool {
	fmt.Println("Buscando todos os Endereços que serão analisados")
	allAddrs := GetAllAddrAnalisado(ConnectionMongoDB, DataBaseAnalise, CollectionAnaliseAwaitingProcessing)
	addrsBaixados := GetAllAddrs(ConnectionMongoDB, DataBaseAnalise, CollectionAddr)

	addrAnalisados := ToAddrAnalisados(allAddrs, addrsBaixados)

	for _, item := range addrAnalisados {
		fmt.Println("Buscando na API o endereço ", item)
		addr := GetAddr(item, urlAPI, RawAddr)

		if len(addr.Address) > 0 && len(addr.Txs) > 0 && len(addr.Txs) == addr.N_tx {
			fmt.Println("Salvando o Endereco no MongoDB ")
			confirmSalve := SalveAddrMongoDB(addr, ConnectionMongoDB, DataBaseAddr, CollectionAddr)
			if confirmSalve {
				fmt.Println("Endereco Salvo ", item)
			} else {
				fmt.Println("Falha ao salva endereco ", item)
				return false
			}
		}
	}
	return true
}

// Função consultando um multiAddr e converter para Addr para salvar no Mongo
func ProcessMAdressesAnalysis(ConnectionMongoDB, DataBaseAnalise, CollectionAnaliseAwaitingProcessing, CollectionAnaliseProcessed, DataBaseAddr, CollectionAddr, urlAPI, RawAddr string) bool {
	fmt.Println("Buscando todos os Endereços que serão analisados")
	addrAnalisados := GetAllAddrAnalisado(ConnectionMongoDB, DataBaseAnalise, CollectionAnaliseAwaitingProcessing)

	for _, item := range addrAnalisados {
		fmt.Println("Buscando na API o endereço ", item.Address)
		Multiaddr := GetMultiAddr([]string{item.Address}, urlAPI, RawAddr, 0, 0)

		addr := Model.UnicoEndereco{
			Hash160:        "",
			N_tx:           Multiaddr.Addresses[0].N_tx,
			Address:        Multiaddr.Addresses[0].Address,
			Final_balance:  Multiaddr.Addresses[0].Final_balance,
			Total_received: Multiaddr.Addresses[0].Total_received,
			Total_sent:     Multiaddr.Addresses[0].Total_sent,
			Txs:            Multiaddr.Txs,
		}

		if len(Multiaddr.Addresses) > 0 && len(Multiaddr.Txs) > 0 {
			if len(addr.Address) > 0 && len(addr.Txs) > 0 {
				fmt.Println("Salvando no MongoDB o Endereco")
				confirmSalve := SalveAddrMongoDB(addr, ConnectionMongoDB, DataBaseAddr, CollectionAddr)
				if confirmSalve {
					fmt.Println("Endereco Salvo ", item.Address)
					fmt.Println("Mudança de Status awaitingProcessing -> Processed")
					confirm := SalveAddrAnalisadoMongoDB(item, ConnectionMongoDB, DataBaseAnalise, CollectionAnaliseProcessed)
					if confirm {
						fmt.Println("Salvo com Sucesso")
						fmt.Println("Deletando endereco da Collection ", CollectionAnaliseAwaitingProcessing)
						confirmDelete := DeleteAddrAnalisado(item.Address, ConnectionMongoDB, DataBaseAnalise, CollectionAnaliseAwaitingProcessing)
						if confirmDelete {
							fmt.Println("Deletado com Sucesso ")
							return true
						} else {
							fmt.Println("Falha ao deletar ", item.Address, " da Collection ", CollectionAnaliseAwaitingProcessing)
							return false
						}
					} else {
						fmt.Println("Não foi Salvo")
						return false
					}

				} else {
					fmt.Println("Falha ao salva endereco ", item.Address)
					return false
				}
			}
		} else {
			return false
		}

	}

	return false
}

func SaveAddressInit(addrs []interface{}, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	if len(addrs) > 0 {
		cliente, contexto, cancel, errou := Database.Connect(ConnectionMongoDB)
		if errou != nil {
			panic(errou)
		}

		Database.Ping(cliente, contexto)
		defer Database.Close(cliente, contexto, cancel)

		result, err := Database.InsertMany(cliente, contexto, DataBaseMongo, Collection, addrs)
		// handle the error
		if err != nil {

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

func GetAddressInitLimitoffset(limit, offset int64, ConnectionMongoDB string, DataBaseMongo string, Collection string) (result []Model.AddressAnalisado) {
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		panic(err)
	}
	defer Database.Close(client, ctx, cancel)
	var filter, option interface{}
	filter = bson.M{}
	option = bson.M{}
	cursor, err := Database.QueryLimitOffset(client, ctx, DataBaseMongo,
		Collection, limit, offset, filter, option)

	if err != nil {
		panic(err)
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		var cluster Model.AddressAnalisado

		if err := cursor.Decode(&cluster); err != nil {
			log.Fatal(err)
		}
		result = append(result, cluster)
	}
	return result
}

func GetAllAddressInit(ConnectionMongoDB, DB1, CollectionCluster string) (result []Model.AddressAnalisado) {
	var limit int64 = 2000
	var offset int64 = 0
	for {
		addrs := GetAddressInitLimitoffset(limit, offset, ConnectionMongoDB, DB1, CollectionCluster)

		if len(addrs) == 0 {
			offset = 0
			break
		}
		result = append(result, addrs...)
		offset = offset + int64(len(addrs))
	}
	return result
}

func GetAddressInitSalvos(ConnectionMongoDB, DB1, CollectionCluster string) map[string]string {
	result := map[string]string{}

	addrs := GetAllAddressInit(ConnectionMongoDB, DB1, CollectionCluster)

	for _, item := range addrs {
		_, ok := result[item.Address]
		if ok {
			continue
		} else {
			result[item.Address] = item.Address
		}
	}
	return result
}

func GetEnderecosQueSeraoSalvos(enderecosInicias []string, enderecosSalvos map[string]string) []interface{} {
	listaEnderecosAnalisados := []interface{}{}

	for _, item := range enderecosInicias {
		_, ok := enderecosSalvos[item]
		if ok {
			continue
		} else {
			listaEnderecosAnalisados = append(listaEnderecosAnalisados,
				Model.AddressAnalisado{
					Address: item,
				})
		}
	}
	return listaEnderecosAnalisados
}
