package Function

import (
	"Tcc/Shared/API"
	"Tcc/Shared/Database"
	"Tcc/Shared/Model"
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"log"
	"strconv"
	"time"
)

// Funções para Distancia

func SaveDistancia(analise Model.Distancia, ConnectionMongoDB string, DataBaseMongo string, Collection string) (sucesso bool, erro bool) {
	if len(analise.AddressInput) > 0 {
		cliente, contexto, cancel, errou := Database.Connect(ConnectionMongoDB)
		if errou != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go}  que esta sendo chamada na Função SaveDistancia - {Function/Distance.go}")
			fmt.Println(errou)
			return false, true
		}

		Database.Ping(cliente, contexto)
		defer Database.Close(cliente, contexto, cancel)

		Database.ToDoc(analise)

		result, err := Database.InsertOne(cliente, contexto, DataBaseMongo, Collection, analise)

		// handle the error
		if err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função InsertOne - {Database/Mongo.go}  que esta sendo chamada na Função SaveDistancia - {Function/Distance.go}")
			fmt.Println(err)
			return false, true
		}

		if result.InsertedID != nil {
			return true, false
		} else {
			return false, true
		}

	} else {
		fmt.Println("Addr esta vazio -", analise.AddressInput, "-")
		return false, false
	}
}

func SalveAllDistanciaMongo(distancias []Model.Distancia, ConnectionMongoDB, DataBaseMongo, Collection string) bool {
	for _, item := range distancias {
		salvo, _, existente := SalveDistanciaMongoDB(item, ConnectionMongoDB, DataBaseMongo, Collection)

		if !salvo && !existente {
			return false
		}
	}
	fmt.Println("Todas as distancias foram salvas")
	return true
}

func SalveDistanciaMongoDB(analise Model.Distancia, ConnectionMongoDB, DataBaseMongo, Collection string) (salvo bool, erro bool, existente bool) {
	confirm := CheckDistancia(ConnectionMongoDB, DataBaseMongo, Collection, "addressinput", analise.AddressInput)
	if confirm {
		fmt.Println("Esse addr ja existe nessa Collection: ", Collection)
		return false, false, true
	}
	salvo, erro = SaveDistancia(analise, ConnectionMongoDB, DataBaseMongo, Collection)
	return salvo, erro, false
}

func GetDistanciaMongoDB(ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (analise Model.Distancia) {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go}  que esta sendo chamada na Função GetDistanciaMongoDB - {Function/Distance.go}")
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
		fmt.Println("Erro na resposta da função Query - {Database/Mongo.go}  que esta sendo chamada na Função GetDistanciaMongoDB - {Function/Distance.go}")
		fmt.Println()

		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {

		if err := cursor.Decode(&analise); err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Decode que esta sendo chamada na Função GetDistanciaMongoDB - {Function/Distance.go}")
			fmt.Println()

			log.Fatal(err)
		}

		return analise
	}

	return Model.Distancia{}
}

func CheckDistancia(ConnectionMongoDB, dataBase, col, key, code string) bool {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função CheckDistancia - {Function/Distance.go}")
		fmt.Println()
		panic(err)
	}

	// Free the resource when mainn dunction is  returned
	defer Database.Close(client, ctx, cancel)
	count, err := Database.CountElemento(client, ctx, dataBase, col, key, code)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função CountElemento - {Database/Mongo.go} que esta sendo chamada na Função CheckDistancia - {Function/Distance.go}")
		fmt.Println()
		panic(err)
	}
	if count > 0 {
		return true
	} else {
		return false
	}
}

func DeleteDistanciaMongo(addr string, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	confirm := CheckDistancia(ConnectionMongoDB, DataBaseMongo, Collection, "addressinput", addr)
	if !confirm {
		fmt.Println("Esse Addr não existe nessa Collection, por isso não tem como excluir: ", Collection)
		return false
	}
	return DeleteDistancia(addr, ConnectionMongoDB, DataBaseMongo, Collection)
}

func DeleteDistancia(analise string, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função DeleteDistancia - {Function/Distance.go}")
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
		"addressinput": analise,
	}

	cursor, err := Database.DeleteOne(client, ctx, DataBaseMongo, Collection, filter)

	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função DeleteOne - {Database/Mongo.go} que esta sendo chamada na Função DeleteDistancia - {Function/Distance.go}")
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

func GetAllDistancia(ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (analise []Model.Distancia) {

	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go}  que esta sendo chamada na Função GetAllDistancia - {Function/Distance.go}")
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
		fmt.Println("Erro na resposta da função Query - {Database/Mongo.go}  que esta sendo chamada na Função GetAllDistancia - {Function/Distance.go}")
		fmt.Println()

		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var temp Model.Distancia

		if err := cursor.Decode(&temp); err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Decode que esta sendo chamada na Função GetAllAddr - {Function/Addr.go}")
			fmt.Println()

			log.Fatal(err)
		}

		analise = append(analise, temp)

	}

	return analise
}

func CreateDistance(ConnectionMongoDB,
	DataBaseAddr, awaitingProcessingEnderecosEmAnalise, processedEnderecosEmAnalise,
	awaitingProcessingAddr, processingAddr, processedAddr,
	DataBaseDistancia, awaitingProcessingDistancia, processedDistancia string, tempo int) bool {

	count := 0

	fmt.Println("Buscando Addr que são dos EnderecosEmAnalise -Nivel 0-")
	enderecosEmAnalise := GetAllAddr(ConnectionMongoDB, DataBaseAddr, awaitingProcessingEnderecosEmAnalise)
	fmt.Println()
	if len(enderecosEmAnalise) > 0 {
		fmt.Println("--------------------INICIO--------------------------------")
		fmt.Println("----Processamento de Endereços que estão no Nível 0-------")

		for _, enderecoEmAnalise := range enderecosEmAnalise {
			for _, tx := range enderecoEmAnalise.Txs {
				for _, input := range tx.Inputs {
					distancia := Model.Distancia{
						Distancia:       0,
						Baixou:          false,
						AddressInput:    input.Prev_Out.Addr,
						AddressAnterior: enderecoEmAnalise.Address,
						AddressNivel0:   enderecoEmAnalise.Address,
					}
					confirm, _, existente := SalveDistanciaMongoDB(distancia, ConnectionMongoDB, DataBaseDistancia, awaitingProcessingDistancia)

					if !confirm && existente {
						fmt.Println("Falha ao salvar distancia, porque ja existe")
					} else if !confirm && !existente {
						fmt.Println("Falha: Não foi salvo distancia")
						return false
					} else {
						fmt.Println("Distancia salva com sucesso AddressInput: ", distancia.AddressInput)
						count++
					}

					fmt.Println()
				}
			}
			fmt.Println()
			fmt.Println("Lista de Txs do endereco: ", enderecoEmAnalise.Address, " foi processado")
			fmt.Println("Todos os enderecos nos inputs de todas as transações foram tratadas")
			fmt.Println("Mudança de Status do Endereco awaitingProcessingEnderecosEmAnalise -> processedEnderecosEmAnalise")
			confirmSalve := SalveAddrMongoDB(enderecoEmAnalise, ConnectionMongoDB, DataBaseAddr, processedEnderecosEmAnalise)
			if confirmSalve {
				fmt.Println("Salvo com Sucesso em processedEnderecosEmAnalise")
				confirmDelete := DeleteAddrMongo(enderecoEmAnalise.Address, ConnectionMongoDB, DataBaseAddr, awaitingProcessingEnderecosEmAnalise)
				if confirmDelete {
					fmt.Println("Deletado com sucesso de awaitingProcessingEnderecosEmAnalise")
					fmt.Println("Mudança de Status concluída")
				}
			} else {
				fmt.Println("Nao foi salvo")
				return false
			}
			fmt.Println()
		}
		fmt.Println("---Foram salvas ", count, " na collection ", awaitingProcessingDistancia, "--")
		fmt.Println("--------------------FIM-----------------------------")
		fmt.Println("--------------- Dormindo ", tempo, " segundos ------")
		time.Sleep(time.Second * time.Duration(tempo))
	} else {
		fmt.Println("Buscando Addr que são de outros Niveis")
		fmt.Println()
		fmt.Println("--------------------INICIO--------------------------------")
		fmt.Println("---Processamento de Endereços que estão em outros níveis--")
		for {
			enderecos := GetAllAddr(ConnectionMongoDB, DataBaseAddr, processingAddr)
			if len(enderecos) < 1 {
				addr := GetAddrMongoDB(ConnectionMongoDB, DataBaseAddr, awaitingProcessingAddr)

				if len(addr.Address) < 1 {
					break
				}

				mudou := MudancaStatusAddr_AP_P(addr, ConnectionMongoDB, DataBaseAddr, awaitingProcessingAddr, processingAddr)

				if !mudou {
					fmt.Println("Erro na mudança de Status awaitingProcessing -> processing")
					return false
				}
			} else {
				indice := GetIndiceLogIndice("..\\Tcc\\IndiceInput.txt")
				// buscando a distancia desse endereco
				// para sabermos qual a distancia dos enderecos de input
				tempDistancia := SearchDistancia(enderecos[0].Address, ConnectionMongoDB, DataBaseDistancia, processedDistancia)
				distance, addressNivel0 := MaxDistance(tempDistancia)

				addrInputs := GetAllInputs(enderecos[0])
				tamanhoAddrInputs := len(addrInputs)
				for index := indice; index < tamanhoAddrInputs; index++ {
					fmt.Println()
					fmt.Println("Tamanho da lista de Inputs: ", tamanhoAddrInputs)
					fmt.Println("Numero do AddrInput: ", index)
					distancia := Model.Distancia{
						Distancia:       distance + 1,
						Baixou:          false,
						AddressInput:    addrInputs[index],
						AddressAnterior: enderecos[0].Address,
						AddressNivel0:   addressNivel0,
					}
					confirm, _, existente := SalveDistanciaMongoDB(distancia, ConnectionMongoDB, DataBaseDistancia, awaitingProcessingDistancia)

					if !confirm && existente {
						fmt.Println()
						fmt.Println("Faltam ", tamanhoAddrInputs-(index+1), " Endereços para encerrar essa lista")
						fmt.Println("Falha ao salvar distancia, porque ja existe")
						fmt.Println()
						fmt.Println("---Foram salvas ", count, " na collection ", awaitingProcessingDistancia, "--")
					} else if !confirm && !existente {
						fmt.Println("Falha: Não foi salvo distancia")
						return false
					} else {
						fmt.Println()
						count++
						fmt.Println("index: ", index+1)
						fmt.Println("Faltam ", tamanhoAddrInputs-(index+1), " Endereços para encerrar essa lista")
						fmt.Println("Distancia salva com sucesso AddressInput: ", distancia.AddressInput)
						fmt.Println("---Foram salvas ", count, " na collection ", awaitingProcessingDistancia, "--")

					}
					indiceTemp := []string{strconv.Itoa(index + 1)}
					EscreverTexto(indiceTemp, "..\\Tcc\\IndiceInput.txt")
					time.Sleep(time.Second * time.Duration(tempo))
				}

				mudou := MudancaStatusAddr_Processing_Processed(enderecos[0], ConnectionMongoDB, DataBaseAddr, processingAddr, processedAddr)

				if !mudou {
					fmt.Println()
					return mudou
				}
				temp := []string{strconv.Itoa(0)}
				EscreverTexto(temp, "..\\Tcc\\IndiceInput.txt")

			}

			fmt.Println("---Foram salvas ", count, " na collection distancia--")
			fmt.Println("--------------------FIM-----------------------------")
			fmt.Println("--------------- Dormindo ", tempo, " segundos ------")
			time.Sleep(time.Second * time.Duration(tempo))
		}
	}

	return true
}

func GetAllInputs(addr Model.UnicoEndereco) (inputs []string) {

	for _, tx := range addr.Txs {
		for _, input := range tx.Inputs {
			if !Contains(inputs, input.Prev_Out.Addr) && len(input.Prev_Out.Addr) > 0 {
				inputs = append(inputs, input.Prev_Out.Addr)
			}
		}
	}

	return inputs
}

func MaxDistance(distancias []Model.Distancia) (max int, addressNivel0 string) {
	max = 0
	if len(distancias) > 0 {
		for _, item := range distancias {
			if item.Distancia >= max {
				max = item.Distancia
				addressNivel0 = item.AddressNivel0
			}
		}
	}
	return max, addressNivel0
}

func SearchDistancia(addr string, ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (result []Model.Distancia) {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função Connect - {Database/Mongo.go} que esta sendo chamada na Função SearchDistancia - {Function/AdressesAnalysis.go}")
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
		"addressinput": addr,
	}

	option = bson.M{}

	cursor, err := Database.Query(client, ctx, DataBaseMongo, CollectionRecuperaDados, filter, option)

	// handle the errors.
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função Query - {Database/Mongo.go} que esta sendo chamada na Função SearchDistancia - {Function/AdressesAnalysis.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {
		var addr Model.Distancia

		if err := cursor.Decode(&addr); err != nil {

			fmt.Println()
			fmt.Println("Erro na resposta da função Decode que esta sendo chamada na Função SearchDistancia - {Function/AdressesAnalysis.go}")
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

func ContainsDistancia(distancia Model.Distancia, distancias []Model.Distancia) bool {

	for _, item := range distancias {
		if distancia.AddressInput == item.AddressInput {
			return true
		}
	}

	return false
}

func ProcessDistance(ConnectionMongoDB,
	DataBaseAddr, awaitingProcessing, processedAddr,
	DataBaseDistancia, processingDistancia, processedDistancia,
	urlAPI, RawAddr, MultiAddr string) (Processado bool, EncerraExecucao bool, erro bool) {
	distancia := GetDistanciaMongoDB(ConnectionMongoDB, DataBaseDistancia, processingDistancia)

	var addr Model.Endereco

	checkAddrAwaitingProcessing := CheckAddr(ConnectionMongoDB, DataBaseAddr, awaitingProcessing, "address", distancia.AddressInput)
	checkAddrProcessed := CheckAddr(ConnectionMongoDB, DataBaseAddr, processedAddr, "address", distancia.AddressInput)

	if !checkAddrAwaitingProcessing && !checkAddrProcessed {
		addr = API.GetEndereco(distancia.AddressInput, urlAPI, RawAddr, 0, 0)

		if len(addr.Address) > 0 {
			fmt.Println("Salvando o endereco: ", addr.Address)
			salvaAddr := SalveAddrSimplicadoMongoDB(addr, ConnectionMongoDB, DataBaseAddr, awaitingProcessing)

			if salvaAddr {
				mudou := MudancaStatusDistance(distancia, ConnectionMongoDB, DataBaseDistancia, processedDistancia, processingDistancia)

				if mudou {
					return true, false, false
				} else {
					return false, true, true
				}

			} else {
				fmt.Println("Endereço retornado da API não foi salvo")
				return false, true, true
			}
		} else {

			MultiAddr := API.GetMultiAddr([]string{distancia.AddressInput}, urlAPI, MultiAddr, 0, 0)

			if MultiAddr.Txs == nil {
				fmt.Println("O valor MultiAddr retornado da API esta vazio")
				return false, false, true
			}
			addr := ConverteMultiAddrParaAddr(MultiAddr)

			salvaAddr := SalveAddrSimplicadoMongoDB(addr, ConnectionMongoDB, DataBaseAddr, awaitingProcessing)

			if salvaAddr {
				mudou := MudancaStatusDistance(distancia, ConnectionMongoDB, DataBaseDistancia, processedDistancia, processingDistancia)
				if mudou {
					return true, false, false
				} else {
					return false, true, true
				}
			} else {
				fmt.Println("Endereço retornado da API não foi salvo")
				return false, true, false
			}

		}

	} else {
		fmt.Println("Endereço ja foi salvo, por isso não precisa para a execução")
		fmt.Println("É necessário mudar o status da distancia")
		mudou := MudancaStatusDistance(distancia, ConnectionMongoDB, DataBaseDistancia, processedDistancia, processingDistancia)

		if mudou {
			return true, false, false
		} else {
			return false, true, true
		}
	}
	return false, true, false
}

func MudancaStatusDistance(distancia Model.Distancia, ConnectionMongoDB, DataBaseDistancia, processedDistancia, processingDistancia string) bool {
	distancia.Baixou = true
	salvo, _ := SaveDistancia(distancia, ConnectionMongoDB, DataBaseDistancia, processedDistancia)

	if !salvo {
		fmt.Println("Não foi salvo com Sucesso")
		return false
	} else {
		fmt.Println("Salvo com sucesso a distancia na collection processed")
	}

	deletado := DeleteDistanciaMongo(distancia.AddressInput, ConnectionMongoDB, DataBaseDistancia, processingDistancia)

	if !deletado {
		fmt.Println("AddressInput: ", distancia.AddressInput, " não foi deletado de processing")
		return false
	} else {
		fmt.Println("Deletado com sucesso a distancia da collection processing")
	}

	return true
}

func ConverteMultiAddrParaAddr(Multiaddr Model.MultiEndereco) (addr Model.Endereco) {

	if len(Multiaddr.Addresses) < 1 ||  len(Multiaddr.Addresses[0].Address) < 1  || len(Multiaddr.Txs) < 1{
		return addr
	}

	addr = Model.Endereco{
		Hash160:        "",
		N_tx:           Multiaddr.Addresses[0].N_tx,
		Address:        Multiaddr.Addresses[0].Address,
		//Final_balance:  Multiaddr.Addresses[0].Final_balance,
		//Total_received: Multiaddr.Addresses[0].Total_received,
		//Total_sent:     Multiaddr.Addresses[0].Total_sent,
	}

	for _, tx := range Multiaddr.Txs {
		var inputs []Model.ListInputs

		for _, input := range tx.Inputs {

			item := Model.PreOut{
				Addr: input.Prev_Out.Addr,
			}

			elem := Model.ListInputs{
				Prev_Out: item,
			}

			inputs = append(inputs, elem)
		}

		txAddr := Model.Tx{
			Hash:     tx.Hash,
			Size:     tx.Size,
			Tx_index: tx.Tx_index,
			Inputs:   inputs,
		}

		addr.Txs = append(addr.Txs, txAddr)
	}

	return addr
}
