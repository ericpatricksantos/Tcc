package main

import (
	"Tcc/Shared/Function"
	"Tcc/Shared/Model"
	"fmt"
	"time"
)

func main() {
	TesteDistancia()
}

func ProcessAdressesAnalysis_v2() {
	ConnectionMongoDB := "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
	AwaitingProcessing := "FaraoEnderecos"
	AwaitingProcessingEnderecosEmAnalise := "Farao"
	urlAPI := "https://blockchain.info"
	RawAddr := "/rawaddr/"
	for {
		confirm := Function.ProcessAdressesAnalysis_v2(ConnectionMongoDB, "Endereco",
			AwaitingProcessing,
			"Endereco", AwaitingProcessingEnderecosEmAnalise, urlAPI, RawAddr)

		if confirm {
			fmt.Println("Endereco Salvo com Sucesso")
		} else {

			break
		}
	}
}

func TesteDistancia1Informacoes() {
	inputs := []string{}
	txs := []string{}
	enderecos := Function.GetAllAddr("mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb",
		"Endereco", "Farao")

	for _, endereco := range enderecos {
		for _, tx := range endereco.Txs {
			txs = append(txs, tx.Hash)
			for _, input := range tx.Inputs {
				inputs = append(inputs, input.Prev_Out.Addr)
			}
		}
	}

	inputUnicos, _ := Function.RemoveDuplicados(inputs)
	txsUnicos, _ := Function.RemoveDuplicados(txs)

	info := Model.Informacoes{
		InfoEnderecos: Model.InfoEnderecos{
			Enderecos:    inputUnicos,
			QtdEnderecos: len(inputUnicos),
		},
		InfoTransacoes: Model.InfoTransacoes{
			Transacoes:    txsUnicos,
			QtdTransacoes: len(txsUnicos),
		},
	}

	Function.SaveInfo(info, "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb",
		"Endereco", "Distancia1Informacoes")
}

func BuscaEnderecosDistancia1_Dividindo_Transacoes() {
	encerraExecucao := false
	urlAPI := "https://blockchain.info"
	ConnectionMongoDB := "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
	Database := "Endereco"
	infoDistancia1 := Function.GetInfoMongoDB(ConnectionMongoDB,
		Database, "Distancia1Informacoes")
	enderecos := infoDistancia1.InfoEnderecos.Enderecos
	tamanhoEnderecos := len(enderecos)
	indiceInicial := Function.BuscaIndice("indice_lista_enderecos.txt")
	tempo := 5

	for indice := indiceInicial; indice < tamanhoEnderecos; indice++ {

		if encerraExecucao {
			break
		}

		if len(enderecos[indice]) < 1 {
			fmt.Println(" Endereco vazio ")
			break
		}

		endereco := enderecos[indice]
		fmt.Println("Indice: ", indice)
		fmt.Println("Buscando o endereco: ", endereco)
		resposta := Function.GetEndereco(endereco, urlAPI, "/rawaddr/", 1000, 0)

		if len(resposta.Address) < 1 || len(resposta.Txs) < 1 {
			fmt.Println(" Erro no retorno das informações do endereço: ", endereco)
			break
		}
		tamanhoTxsRecebidas := 0
		N_tx := resposta.N_tx
		respostaCompleta := resposta

		for {
			if encerraExecucao {
				break
			}
			tamanhoTxsRecebidas = tamanhoTxsRecebidas + len(respostaCompleta.Txs)
			contSalve := 0
			if tamanhoTxsRecebidas == N_tx {
				listaEnderecos := Function.DividiTransacoesDosEndereco(respostaCompleta, 1000.0)
				if listaEnderecos == nil || len(listaEnderecos) == 0 {
					encerraExecucao = true
					break
				}
				tamanholistaEnderecos := len(listaEnderecos)
				fmt.Println("Salvando o Endereco: ", endereco, " no MongoDB ")
				fmt.Println(" Tamanho da lista total de transações: ", N_tx)
				fmt.Println("Tamanho da lista que contem todas as transações do ", endereco, " e ", tamanholistaEnderecos)
				for _, list_end := range listaEnderecos {
					confirmSalve := Function.SaveAddrSimplificado(list_end, ConnectionMongoDB, Database, "Distancia1")

					if confirmSalve {
						contSalve = contSalve + 1
					}
					if contSalve == tamanholistaEnderecos {
						fmt.Println("Endereco Salvo ", endereco)
						Function.IncrementaIndice(indice, "indice_lista_enderecos.txt")
					} else if !confirmSalve {
						fmt.Println("Falha ao salva endereco ", endereco)
						encerraExecucao = true
						break
					}
				}
				if contSalve == tamanholistaEnderecos {
					break
				}
			} else {
				time.Sleep(time.Second * time.Duration(tempo))
				resposta = Function.GetEndereco(endereco, urlAPI, "/rawaddr/", 1000, tamanhoTxsRecebidas)
				if len(resposta.Address) < 1 || len(resposta.Txs) < 1 {
					fmt.Println(" Nao buscou os restantes da transações")
					encerraExecucao = true
					break
				}
				respostaCompleta.Txs = append(respostaCompleta.Txs, resposta.Txs...)
			}

		}
		time.Sleep(time.Second * time.Duration(tempo))
	}

}

// Sendo usado atualmente
func BuscaEnderecosD1() {
	utiliza_multiaddr := false
	limit := 5000
	encerraExecucao := false
	urlAPI := "https://blockchain.info"
	ConnectionMongoDB := "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
	Database := "Endereco"
	Collection := "Endereco_2053"
	//infoDistancia1 := Function.GetInfoMongoDB( ConnectionMongoDB,
	//	Database,"Distancia1Informacoes")
	//enderecos := infoDistancia1.InfoEnderecos.Enderecos
	enderecos := []string{"17kb7c9ndg7ioSuzMWEHWECdEVUegNkcGc"}
	tamanhoEnderecos := len(enderecos)
	indiceInicial := Function.BuscaIndice("indice_lista_enderecos.txt")
	tempo := 10

	for indice := indiceInicial; indice < tamanhoEnderecos; indice++ {

		if encerraExecucao {
			break
		}

		if len(enderecos[indice]) < 1 {
			encerraExecucao = true
			fmt.Println(" Endereco vazio ")
			break
		}
		var resposta Model.Endereco
		var respostaMultiAddr Model.MultiEndereco
		endereco := enderecos[indice]
		fmt.Println("Indice: ", indice)
		fmt.Println("Buscando o endereco: ", endereco)
		offsetTxsSalvas := Function.BuscaIndice("offsetTxs.txt")
		var tamanhoTxsRecebidas int

		if offsetTxsSalvas == 0 {
			tamanhoTxsRecebidas = 0
		} else {
			tamanhoTxsRecebidas = offsetTxsSalvas
		}
		if !utiliza_multiaddr {
			fmt.Println("RawAddr")
			resposta = Function.GetEndereco(endereco, urlAPI, "/rawaddr/", limit, tamanhoTxsRecebidas)
		}
		if len(resposta.Address) < 1 || len(resposta.Txs) < 1 {
			utiliza_multiaddr = true
			time.Sleep(time.Second * time.Duration(tempo))
			fmt.Println("MultiAddr")
			respostaMultiAddr = Function.GetMultiAddr([]string{endereco}, urlAPI, "/multiaddr?active=", limit, tamanhoTxsRecebidas)
			resposta = Function.ConverteMultiAddrParaAddr(respostaMultiAddr)
			if len(resposta.Address) < 1 || len(resposta.Txs) < 1 {
				fmt.Println(" Erro no retorno das informações do endereço: ", endereco)
				encerraExecucao = true
				break
			}
		}

		N_tx := resposta.N_tx
		for {
			if encerraExecucao {
				break
			}
			tamanhoTxsRecebidas = tamanhoTxsRecebidas + len(resposta.Txs)
			confirmSalve := Function.SaveAddrSimplificadoEmPartes(resposta, 2000, ConnectionMongoDB, Database, Collection)

			if confirmSalve {
				fmt.Println("Define offset: ", tamanhoTxsRecebidas)
				Function.DefineIndice(tamanhoTxsRecebidas, "offsetTxs.txt")
				fmt.Println("Endereco Salvo ", endereco)
				fmt.Println("Tamanho da transação: ", N_tx)
				fmt.Println("Transações restantes: ", N_tx-tamanhoTxsRecebidas)
			} else if !confirmSalve {
				fmt.Println("Falha ao salva endereco ", endereco)
				encerraExecucao = true
				break
			}

			if tamanhoTxsRecebidas >= N_tx {
				fmt.Println("Foram salvas todas as transações do endereço: ", endereco)
				fmt.Println("Incrementa o indice, ou seja, passa para o proximo endereço")
				fmt.Println("Inicia o offset")
				Function.IncrementaIndice(indice, "indice_lista_enderecos.txt")
				Function.DefineIndice(0, "offsetTxs.txt")
				break
			} else {
				time.Sleep(time.Second * time.Duration(tempo))
				if !utiliza_multiaddr {
					fmt.Println("RawAddr")
					resposta = Function.GetEndereco(endereco, urlAPI, "/rawaddr/", limit, tamanhoTxsRecebidas)
				}
				if len(resposta.Address) < 1 || len(resposta.Txs) < 1 {
					utiliza_multiaddr = true
					time.Sleep(time.Second * time.Duration(tempo))
					fmt.Println("MultiAddr")
					respostaMultiAddr = Function.GetMultiAddr([]string{endereco}, urlAPI, "/multiaddr?active=", limit, tamanhoTxsRecebidas)
					resposta = Function.ConverteMultiAddrParaAddr(respostaMultiAddr)
					if len(resposta.Address) < 1 || len(resposta.Txs) < 1 {
						fmt.Println(" Nao buscou os restantes da transações do endereço: ", endereco)
						fmt.Println("Limit: ", limit)
						fmt.Println("Offset: ", tamanhoTxsRecebidas)
						encerraExecucao = true
						break
					}
				}
			}
		}
		time.Sleep(time.Second * time.Duration(tempo))
	}
}

func TesteEndereco_indice_2053() {
	ConnectionMongoDB := "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
	Database := "Endereco"
	Collection := "Endereco_2053"
	enderecos := Function.GetAllAddrLimit(1000000, ConnectionMongoDB,
		Database, Collection)
	dic := map[string]string{}
	hashes := []string{}
	for _, addr := range enderecos {
		for _, tx := range addr.Txs {
			hashes = append(hashes, tx.Hash)
			_, ok := dic[tx.Hash]
			if ok {
				continue
			} else {
				dic[tx.Hash] = tx.Hash
			}
		}
	}
	//x,_ := Function.RemoveDuplicados(hashes)

	fmt.Println(len(hashes))
	//fmt.Println(len(x))
	fmt.Println(len(dic))
}

func TesteDistancia() {
	ConnectionMongoDB := "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
	Database := "Endereco"
	Collection := "Distancia1"

	valores := Function.GetAllAddrLimit(100000000, ConnectionMongoDB, Database, Collection)
	dic := map[string]string{}

	for _, item := range valores {
		_, ok := dic[item.Address]
		if ok {

		} else {
			dic[item.Address] = item.Address
		}
	}

	fmt.Println(len(dic))
}

func BuscaEnderecosDistancia1() {
	fmt.Println("-- MultiAddr ---")
	BuscaEnderecosDistancia1_MultiAddr()
	fmt.Println("-- RawAddr ---")
	BuscaEnderecosDistancia1_RawAddr()
}

func BuscaEnderecosDistancia1_RawAddr() {
	encerraExecucao := false
	urlAPI := "https://blockchain.info"
	ConnectionMongoDB := "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
	Database := "Endereco"
	infoDistancia1 := Function.GetInfoMongoDB(ConnectionMongoDB,
		Database, "Distancia1Informacoes")
	enderecos := infoDistancia1.InfoEnderecos.Enderecos
	tamanhoEnderecos := len(enderecos)
	indiceInicial := Function.BuscaIndice("indice_lista_enderecos.txt")
	tempo := 10

	for indice := indiceInicial; indice < tamanhoEnderecos; indice++ {

		if encerraExecucao {
			break
		}

		if len(enderecos[indice]) < 1 {
			encerraExecucao = true
			fmt.Println(" Endereco vazio ")
			break
		}

		endereco := enderecos[indice]
		fmt.Println("Indice: ", indice)
		fmt.Println("Buscando o endereco: ", endereco)
		resposta := Function.GetEndereco(endereco, urlAPI, "/rawaddr/", 2000, 0)

		if len(resposta.Address) < 1 || len(resposta.Txs) < 1 {
			fmt.Println(" Erro no retorno das informações do endereço: ", endereco)
			encerraExecucao = true
			break
		}
		tamanhoTxsRecebidas := 0
		N_tx := resposta.N_tx
		listaEnderecos := []Model.Endereco{}
		listaEnderecos = append(listaEnderecos, resposta)
		fmt.Println("Tamanho das Transações: ", N_tx)
		for {
			if encerraExecucao {
				break
			}
			tamanhoTxsRecebidas = tamanhoTxsRecebidas + len(resposta.Txs)
			contSalve := 0
			if tamanhoTxsRecebidas >= N_tx {
				tamanholistaEnderecos := len(listaEnderecos)
				fmt.Println("Salvando o Endereco: ", endereco, " no MongoDB ")
				fmt.Println("Tamanho da lista que contem todas as transações do ", endereco, " é ", tamanholistaEnderecos)
				for _, list_end := range listaEnderecos {
					confirmSalve := Function.SaveAddrSimplificado(list_end, ConnectionMongoDB, Database, "Distancia1")

					if confirmSalve {
						contSalve = contSalve + 1
					}
					if contSalve == tamanholistaEnderecos {
						fmt.Println("Endereco Salvo ", endereco)
						Function.IncrementaIndice(indice, "indice_lista_enderecos.txt")
					} else if !confirmSalve {
						fmt.Println("Falha ao salva endereco ", endereco)
						encerraExecucao = true
						break
					}
				}
				if contSalve == tamanholistaEnderecos {
					break
				}
			} else {
				time.Sleep(time.Second * time.Duration(tempo))
				resposta = Function.GetEndereco(endereco, urlAPI, "/rawaddr/", 2000, tamanhoTxsRecebidas)
				if len(resposta.Address) < 1 || len(resposta.Txs) < 1 {
					fmt.Println(" Nao buscou os restantes da transações do endereço: ", endereco)
					fmt.Println("Limit: ", 2000)
					fmt.Println("Offset: ", tamanhoTxsRecebidas)
					encerraExecucao = true
					break
				}
				listaEnderecos = append(listaEnderecos, resposta)
			}

		}
		time.Sleep(time.Second * time.Duration(tempo))
	}

}

func BuscaEnderecosDistancia1_MultiAddr() {
	encerraExecucao := false
	urlAPI := "https://blockchain.info"
	ConnectionMongoDB := "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
	Database := "Endereco"
	infoDistancia1 := Function.GetInfoMongoDB(ConnectionMongoDB,
		Database, "Distancia1Informacoes")
	enderecos := infoDistancia1.InfoEnderecos.Enderecos
	tamanhoEnderecos := len(enderecos)
	indiceInicial := Function.BuscaIndice("indice_lista_enderecos.txt")
	tempo := 10

	for indice := indiceInicial; indice < tamanhoEnderecos; indice++ {

		if encerraExecucao {
			break
		}

		if len(enderecos[indice]) < 1 {
			encerraExecucao = true
			fmt.Println(" Endereco vazio ")
			break
		}

		endereco := enderecos[indice]
		fmt.Println("Indice: ", indice)
		fmt.Println("Buscando o endereco: ", endereco)
		respostaMultiAddr := Function.GetMultiAddr([]string{endereco}, urlAPI, "/multiaddr?active=", 2000, 0)
		resposta := Function.ConverteMultiAddrParaAddr(respostaMultiAddr)
		if len(resposta.Address) < 1 || len(resposta.Txs) < 1 {
			fmt.Println(" Erro no retorno das informações do endereço: ", endereco)
			encerraExecucao = true
			break
		}
		tamanhoTxsRecebidas := 0
		N_tx := resposta.N_tx
		listaEnderecos := []Model.Endereco{}
		listaEnderecos = append(listaEnderecos, resposta)
		fmt.Println("Tamanho das Transações: ", N_tx)
		for {
			if encerraExecucao {
				break
			}
			tamanhoTxsRecebidas = tamanhoTxsRecebidas + len(resposta.Txs)
			contSalve := 0
			if tamanhoTxsRecebidas >= N_tx {
				tamanholistaEnderecos := len(listaEnderecos)
				fmt.Println("Salvando o Endereco: ", endereco, " no MongoDB ")
				fmt.Println("Tamanho da lista que contem todas as transações do ", endereco, " é ", tamanholistaEnderecos)
				for _, list_end := range listaEnderecos {
					confirmSalve := Function.SaveAddrSimplificado(list_end, ConnectionMongoDB, Database, "Distancia1")

					if confirmSalve {
						contSalve = contSalve + 1
					}
					if contSalve == tamanholistaEnderecos {
						Function.IncrementaIndice(indice, "indice_lista_enderecos.txt")
						fmt.Println("Endereco Salvo ", endereco)
					} else if !confirmSalve {
						fmt.Println("Falha ao salva endereco ", endereco)
						encerraExecucao = true
						break
					}
				}
				if contSalve == tamanholistaEnderecos {
					break
				}
			} else {
				time.Sleep(time.Second * time.Duration(tempo))
				respostaMultiAddr = Function.GetMultiAddr([]string{endereco}, urlAPI, "/multiaddr?active=", 2000, tamanhoTxsRecebidas)
				resposta = Function.ConverteMultiAddrParaAddr(respostaMultiAddr)
				if len(resposta.Address) < 1 || len(resposta.Txs) < 1 {
					fmt.Println(" Nao buscou os restantes da transações do endereço: ", endereco)
					fmt.Println("Limit: ", 100)
					fmt.Println("Offset: ", tamanhoTxsRecebidas)
					encerraExecucao = true
					break
				}
				listaEnderecos = append(listaEnderecos, resposta)
			}

		}
		time.Sleep(time.Second * time.Duration(tempo))
	}

}
