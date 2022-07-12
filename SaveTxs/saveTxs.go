package main

import (
	"Tcc/Shared/Function"
	"Tcc/Shared/Model"
	"fmt"
	"time"
)

var applicationStateFile = "./ApplicationStateFile/"

var ConnectionMongoDB string             // String de Conexão com o MongoDB
var urlAPI string                        // Url base da API do blockchain
var RawAddr = "/rawaddr/"                // End point
var multiAddr = "/multiaddr?active="     // End point
var DBEnderecoFarao string               // Database que contem os endereços do farao dos bitcoins
var DBTransacoesEndereco string          // Database que contem as transações do farao dos bitcoins
var DBDistancia1Informacoes string       // Database que contem as informações dos endereços que estão na Distância 1
var ColEnderecosFaraoDosBitcoins string  // Collection que sera usada para salvar os endereços do farao dos bitcoins
var ColTransacoesFaraoDosBitcoins string // Collection que será usada para salvar as transações do farao dos bitcoins
var ColDistancia1Info string             // Collection que sera usada para salvar as informações da Distancia 1
var ColDistancia1 string                 // Collection que sera usada para salvar as transações dos endereços que estão na Distancia 1
var IndiceListaEnderecos string          // Salva o indice da lista de endereços
var Offset string                        // Usado pular as transações que foram salvas
var EnderecosIniciaistxt string          // Arquivo que contem os endereços iniciai(endereços do farao dos bitcoins) que serão salvos no MongoDB
func init() {
	ConnectionMongoDB = "mongodb://127.0.0.1:27017/"
	urlAPI = "https://blockchain.info"

	DBEnderecoFarao = "Endereco"
	DBTransacoesEndereco = "Endereco"
	DBDistancia1Informacoes = "Endereco"

	ColEnderecosFaraoDosBitcoins = "FaraoEnderecos"
	ColTransacoesFaraoDosBitcoins = "Farao"
	ColDistancia1Info = "Distancia1Informacoes"
	ColDistancia1 = "Distancia1"

	EnderecosIniciaistxt = "enderecosIniciais.txt"
	IndiceListaEnderecos = applicationStateFile + "indice_lista_enderecos.txt"
	Offset = applicationStateFile + "offsetTxs.txt"

	fmt.Println("Criando os arquivos da aplicação")
	Function.CreateListFile([]string{IndiceListaEnderecos, Offset})
}

func main() {
	fmt.Println("Salvando os endereços do Faraó dos Bitcoins no MongoDB")
	SaveEnderecosFaraoBitcoins()
	fmt.Println("Iniciando a busca pelas transações dos endereços do Farao dos Bitcoins")
	ProcessAdressesAnalysis_v2()
	fmt.Println("Criando uma lista de endereços que estão na Distância 1")
	CriacaoDistancia1Informacoes()
	fmt.Println("Iniciando a busca pelas transações dos endereços que estão na Distância 1")
	BuscaTransacoesDosEnderecosDaDistancia1()
}

func SaveEnderecosFaraoBitcoins() {
	enderecosInicias, _ := Function.LerTexto(EnderecosIniciaistxt)
	enderecosSalvos := Function.GetAddressInitSalvos(ConnectionMongoDB, DBEnderecoFarao, ColEnderecosFaraoDosBitcoins)

	listaEnderecosAnalisados := Function.GetEnderecosQueSeraoSalvos(enderecosInicias, enderecosSalvos)

	if len(listaEnderecosAnalisados) == 0 {
		fmt.Println("Todos os endereços iniciais foram salvos anteriormente")
	} else if len(listaEnderecosAnalisados) > 0 {
		confirm := Function.SaveAddressInit(listaEnderecosAnalisados, ConnectionMongoDB, DBEnderecoFarao, ColEnderecosFaraoDosBitcoins)
		if confirm {
			fmt.Println("Salvo com sucesso os endereços iniciais")
			fmt.Println()
		} else {
			fmt.Println("Erro ao Salvar")
		}
	}
}

func ProcessAdressesAnalysis_v2() {
	for {
		confirm := Function.ProcessAdressesAnalysis_v2(ConnectionMongoDB, DBEnderecoFarao,
			ColEnderecosFaraoDosBitcoins, DBTransacoesEndereco,
			ColTransacoesFaraoDosBitcoins, urlAPI, RawAddr)
		if confirm {
			fmt.Println("Enderecos Salvo com Sucesso")
			break
		} else {
			panic("Erro: Ao buscar as transações dos endereços iniciais")
		}
	}
}

func BuscaTransacoesDosEnderecosDaDistancia1() {
	utiliza_multiaddr := false
	limit := 5000
	encerraExecucao := false
	infoDistancia1 := Function.GetInfoMongoDB(ConnectionMongoDB,
		DBTransacoesEndereco, ColDistancia1Info)
	enderecos := infoDistancia1.InfoEnderecos.Enderecos
	tamanhoEnderecos := len(enderecos)
	indiceInicial := Function.BuscaIndice(IndiceListaEnderecos)
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
		offsetTxsSalvas := Function.BuscaIndice(Offset)
		var tamanhoTxsRecebidas int

		if offsetTxsSalvas == 0 {
			tamanhoTxsRecebidas = 0
		} else {
			tamanhoTxsRecebidas = offsetTxsSalvas
		}
		if !utiliza_multiaddr {
			fmt.Println("RawAddr")
			resposta = Function.GetEndereco(endereco, urlAPI, RawAddr, limit, tamanhoTxsRecebidas)
		}
		if len(resposta.Address) < 1 || len(resposta.Txs) < 1 {
			utiliza_multiaddr = true
			time.Sleep(time.Second * time.Duration(tempo))
			fmt.Println("MultiAddr")
			respostaMultiAddr = Function.GetMultiAddr([]string{endereco}, urlAPI, multiAddr, limit, tamanhoTxsRecebidas)
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
			confirmSalve := Function.SaveAddrSimplificadoEmPartes(resposta, 2000, ConnectionMongoDB, DBTransacoesEndereco, ColDistancia1)

			if confirmSalve {
				fmt.Println("Define Offset: ", tamanhoTxsRecebidas)
				Function.DefineIndice(tamanhoTxsRecebidas, Offset)
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
				fmt.Println("Inicia o Offset")
				Function.IncrementaIndice(indice, IndiceListaEnderecos)
				Function.DefineIndice(0, Offset)
				break
			} else {
				time.Sleep(time.Second * time.Duration(tempo))
				if !utiliza_multiaddr {
					fmt.Println("RawAddr")
					resposta = Function.GetEndereco(endereco, urlAPI, RawAddr, limit, tamanhoTxsRecebidas)
				}
				if len(resposta.Address) < 1 || len(resposta.Txs) < 1 {
					utiliza_multiaddr = true
					time.Sleep(time.Second * time.Duration(tempo))
					fmt.Println("MultiAddr")
					respostaMultiAddr = Function.GetMultiAddr([]string{endereco}, urlAPI, multiAddr, limit, tamanhoTxsRecebidas)
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

func CriacaoDistancia1Informacoes() {
	inputs := []string{}
	txs := []string{}
	enderecos := Function.GetAllAddr(ConnectionMongoDB,
		DBTransacoesEndereco, ColTransacoesFaraoDosBitcoins)

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

	Function.SaveInfo(info, ConnectionMongoDB,
		DBDistancia1Informacoes, ColDistancia1Info)
}
