package main

import (
	"Tcc/Shared/Function"
	"Tcc/Shared/Model"
	"fmt"
	"time"
)

func main() {
	fmt.Println("Iniciando a busca pelas transações dos endereos do Farao dos Bitcoins")
	ProcessAdressesAnalysis_v2()
	fmt.Println("Iniciando a busca pelas transações dos endereços que estão na distância 1")
	BuscaTransacoesDosEnderecosDaDistancia1()
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

func BuscaTransacoesDosEnderecosDaDistancia1() {
	utiliza_multiaddr := false
	limit := 5000
	encerraExecucao := false
	urlAPI := "https://blockchain.info"
	ConnectionMongoDB := "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
	Database := ""
	Collection := ""
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
