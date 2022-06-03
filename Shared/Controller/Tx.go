package Controller

import (
	"Tcc/Shared/API"
	"Tcc/Shared/Function"
	"Tcc/Shared/Model"
	"fmt"
	"strconv"
	"time"
)

// SaveTxs Salva todas as transações de um Block no MongoDB
func SaveTxs(Txs []int, urlAPI string, rawTx string, ConnectionMongoDB string, DataBaseMongo string, Collection string, FileLogHash string, indiceInicial int) (TransacaoSalva bool, FinalizaExecucao bool) {
	//indiceInicial := Function.GetIndiceLogIndice(FileLogHash) + 1
	for contador := indiceInicial; contador < len(Txs); contador++ {
		confirm, finalizaExecucao := SaveTx(strconv.Itoa(Txs[contador]), urlAPI, rawTx, ConnectionMongoDB, DataBaseMongo, Collection)
		if !confirm {
			fmt.Println("Não foi salvo a transação ", Txs[contador])
			if !finalizaExecucao {
				fmt.Println("Não é necessario finalizar a execução")
				fmt.Println("Indice Atualizado para ", contador)
				temp := []string{strconv.Itoa(contador)}
				Function.EscreverTexto(temp, FileLogHash)
				return confirm, finalizaExecucao
			}
			return confirm, finalizaExecucao
		} else {
			fmt.Println("Salvo a Transação: Nº ", Txs[contador])
			fmt.Println("Indice Atualizado para ", contador)
			temp := []string{strconv.Itoa(contador)}
			Function.EscreverTexto(temp, FileLogHash)
			return confirm, finalizaExecucao
		}
		//fmt.Println("Salvo a Transação: Nº ", Txs[contador])
		//temp := []string{strconv.Itoa(contador)}
		//Function.EscreverTexto(temp, FileLogHash)
		//fmt.Println("Indice Atualizado para ", contador)
		//
		//time.Sleep(time.Minute * time.Duration(tempo))
	}
	fmt.Println("Indice Atual: ", indiceInicial, "Não foi salvo")
	return false, true
}

// SaveTx Salva as Transações no MongoDb consultando pelo API Blockchain
func SaveTx(hash string, urlAPI string, rawTx string, ConnectionMongoDB string, DataBaseMongo string, Collection string) (TransacaoSalva bool, FinalizaExecucao bool) {
	txIndex, _ := strconv.Atoi(hash)
	check := Function.CheckTxIndex(ConnectionMongoDB, DataBaseMongo, Collection, "tx_index", txIndex)

	if check {
		fmt.Println("TxIndex: ", txIndex)
		fmt.Println("Essa Transação existe na Collection ", Collection)
		return false, false
	}
	tx := GetTx(hash, urlAPI, rawTx)

	if len(tx.Inputs) < 1 && len(tx.Hash) > 0 {

		fmt.Println()
		fmt.Println("O objeto Tx retornado não foi salvo, porque a lista de inputs está vázia")
		fmt.Println("O hash dessa transação foi salvo em TxAddrEmpty para ser verificado")
		Function.EscreverTextoSemApagar([]string{tx.Hash}, "..\\Tcc\\TxAddrEmpty.txt")
		fmt.Println()
		return false, false
	}

	if len(tx.Hash) > 0 {
		resposta := Function.SaveTx(tx, ConnectionMongoDB, DataBaseMongo, Collection)
		if resposta {
			return true, false
		} else {
			return false, true
		}
	} else {
		fmt.Println()
		fmt.Println("Erro: Não foi possível realizar a Requisição pela Transação")
		fmt.Println("Erro: O objeto retornado pela API não é uma Transação válida")
		fmt.Println()
		return false, true
	}
}

// GetTx Get Transação da API da Blockchain
func GetTx(hash, urlAPI, rawTx string) Model.Transaction {
	return API.GetTransaction(hash, urlAPI, rawTx)
}

func SalveTxMongoDB(tx Model.Transaction, ConnectionMongoDB, DataBaseMongo, Collection string) bool {
	result, _ := Function.SalveTxMongoDB(tx, ConnectionMongoDB, DataBaseMongo, Collection)
	return result
}

func DeleteTxMongo(hash string, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	return Function.DeleteTxMongo(hash, ConnectionMongoDB, DataBaseMongo, Collection)
}

func GetTxMongoDB(ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) Model.Transaction {
	return Function.GetTxMongoDB(ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)
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
