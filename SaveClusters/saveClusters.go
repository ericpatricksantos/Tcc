package main

import (
	"Tcc/Shared/Function"
	"Tcc/Shared/Model"
	"fmt"
	"time"
)

var applicationStateFile = "./ApplicationStateFile/"
var limit int64 = 100
var ConnectionMongo string
var DB_Endereco string
var DB_Cluster string
var Collection_Transacao_Distancia1 string
var Collection_Cluster_Identificadores string
var Collection_Cluster_Distancia1 string
var hashParaDeletar string
var indiceSaveAddr string
var offsetAddr string
var Collection_Distancia1 string
var Collection_Cluster_Identificadores_1 string
var Collection_Cluster_Identificadores_2 string
var Collection_Cluster_Distancia2 string
var Col_Cluster_Identificadores string
var Col_Cluster_Clusters string

func init() {
	ConnectionMongo = "mongodb://127.0.0.1:27017/"
	DB_Endereco = "Endereco"                                       // Database que contem os endereços e suas transações
	DB_Cluster = "Cluster"                                         // Database que contem os clusters
	Collection_Transacao_Distancia1 = "Farao"                      // Collection que contem as transações do farao do bitcoins
	Collection_Cluster_Identificadores = "Identificadores_1"       // Collection que contem os identificadores dos clusters da distancia 1
	Collection_Cluster_Distancia1 = "Distancia1"                   // Collection que contem os clusters da distancia 1
	hashParaDeletar = applicationStateFile + "hashParaDeletar.txt" // Arquivo que contem os hash que precisa deletar
	indiceSaveAddr = applicationStateFile + "indiceSaveAddr.txt"   // Armazena o indice do endereço que esta sendo criado o cluster
	offsetAddr = applicationStateFile + "offsetAddr.txt"           // Armazena o tamanho das transações que foram salvas
	Collection_Distancia1 = "Distancia1"                           // Collection que contem os transações endereços da distancia 1
	Collection_Cluster_Identificadores_1 = "Identificadores_1"     // Collection que contem os Identifcadores da distancia 1
	Collection_Cluster_Identificadores_2 = "Identificadores_2"     // Collection que contem os Identifcadores da distancia 2
	Collection_Cluster_Distancia2 = "Distancia2"                   // Collection que contem os clusters da distancia 2
	Col_Cluster_Identificadores = "Identificadores"
	Col_Cluster_Clusters = "Clusters"
	fmt.Println("Criando os arquivos da aplicação")
	Function.CreateListFile([]string{hashParaDeletar, indiceSaveAddr, offsetAddr})
}

func main() {
	fmt.Println("Iniciando a criação dos clusters das transações da Distancia 1")
	CreateClustersDistancia1_Map()
	fmt.Println("Iniciando a criação dos clusters das transações da Distancia 2")
	CreateClustersDistancia2_Map()
	fmt.Println("Transferindo os clusters para a Collection Clusters")
	TransferindoClusters()
}

func CreateClustersDistancia1_Map() {
	encerraExecucao := false
	// as transações dos endereços do faráo estão na distancia 1
	txs_endereco_farao := Function.GetAllAddr(ConnectionMongo, DB_Endereco, Collection_Transacao_Distancia1)

	for _, endereco_farao := range txs_endereco_farao {
		if encerraExecucao {
			break
		}

		for _, tx := range endereco_farao.Txs {
			if encerraExecucao {
				break
			}

			hashTransacao := tx.Hash
			existeCluster := Function.CheckCluster(ConnectionMongo, DB_Cluster,
				Collection_Cluster_Identificadores, "identificador", hashTransacao)
			if existeCluster {
				fmt.Println(" Existe um cluster com essa transação: ", hashTransacao)
				fmt.Println(" Passa para a próxima transação")
			} else {
				cluster := Model.MapCluster{
					Identificador: hashTransacao,
					Clusters:      map[string]string{},
				}
				for _, input := range tx.Inputs {
					endereco_input := input.Prev_Out.Addr
					_, existeEndereco := cluster.Clusters[endereco_input]
					if existeEndereco {
						fmt.Println(" -------- O endereco: ", endereco_input,
							" existe no cluster (Lista da endereços) -----")
					} else {

						cluster.Clusters[endereco_input] = ""
						fmt.Println(" -------- O endereco: ", endereco_input,
							" foi adicionado no cluster (Lista da endereços) -----")
					}
				}

				identificadorCluster := Model.Identificador{
					Identificador:  hashTransacao,
					TamanhoCluster: len(cluster.Clusters),
				}

				confirmSalveCluster := Function.SalvaMapCluster(cluster, ConnectionMongo, DB_Cluster, Collection_Cluster_Distancia1)
				if confirmSalveCluster {
					confirmSalveIdentificadorCluster := Function.SalvaIdentificadorCluster(identificadorCluster, ConnectionMongo, DB_Cluster, Collection_Cluster_Identificadores)
					if confirmSalveIdentificadorCluster {
						fmt.Println(" Salvo com sucesso o cluster e o seu identificador: ", hashTransacao)
					} else {
						fmt.Println(" Finaliza Execução: Erro o cluster e o identificador não foram salvos", hashTransacao)
						encerraExecucao = true
						break
					}
				} else {
					fmt.Println(" Finaliza Execução, Erro o cluster não foi salvo: ", hashTransacao)
					encerraExecucao = true
					break
				}
			}
		}

	}
}

func CreateClustersDistancia2_Map() {

	hashDel, _ := Function.LerTexto(hashParaDeletar)

	if len(hashDel) > 0 {
		panic("Existem valores para apagar")
	}

	encerraExecucao := false

	allIdentificadores := Function.UnionIdentificadoresD1_D2(ConnectionMongo, DB_Cluster, Collection_Cluster_Identificadores_1, Collection_Cluster_Identificadores_2)
	fmt.Println("Tamanho de todos os identificadores: ", len(allIdentificadores))
	for {
		offset := Function.BuscaIndice(offsetAddr)
		indiceInicial := Function.BuscaIndice(indiceSaveAddr)
		if encerraExecucao {
			fmt.Println(" Erro ")
			break
		}

		txs_endereco_d1 := Function.GetAllAddrLimitOffset(limit, int64(offset), ConnectionMongo, DB_Endereco, Collection_Distancia1)
		tamanho_txs_endereco_d1 := len(txs_endereco_d1)
		fmt.Println("Inicio criação de clusters")
		fmt.Println("Tamanho de Endereços obtidos: ", tamanho_txs_endereco_d1)
		fmt.Println()
		if tamanho_txs_endereco_d1 == 0 {
			fmt.Println("--------------- Fim ----------------------")
			encerraExecucao = true
			break
		}
		for indice := indiceInicial; indice < tamanho_txs_endereco_d1; indice++ {

			if encerraExecucao {
				break
			}
			endereco_d1 := txs_endereco_d1[indice]
			endereco := endereco_d1.Address
			IdentificadoresSeraoSalvos := []string{}
			documentsMapCluster := []interface{}{}
			documentsIdentificadores := []interface{}{}

			fmt.Println("Inicio da criação de clusters das transações do endereço: ", endereco)

			for _, tx := range endereco_d1.Txs {
				if encerraExecucao {
					break
				}
				hashTransacao := tx.Hash
				fmt.Println()
				fmt.Println("Verificando se o Cluster: ", hashTransacao, " existe")
				_, existeCluster := allIdentificadores[hashTransacao]
				if existeCluster {
					fmt.Println("Endereço: ", endereco, " Indice: ", indice)
					fmt.Println("Tamanho Txs: ", endereco_d1.N_tx)
					fmt.Println("Existe um cluster com essa transação: ", hashTransacao)
					fmt.Println("Passa para a próxima transação")
				} else {
					cluster := Model.MapCluster{
						Identificador: hashTransacao,
						Clusters:      map[string]string{},
					}
					for _, input := range tx.Inputs {
						endereco_input := input.Prev_Out.Addr
						_, existeEndereco := cluster.Clusters[endereco_input]
						if existeEndereco {
							fmt.Println(" -------- O endereco: ", endereco_input,
								" existe no cluster (Lista da endereços) -----")
							fmt.Println("Endereço: ", endereco, " Indice: ", indice)
							fmt.Println("Tamanho Txs: ", endereco_d1.N_tx)
						} else {
							// cluster.Clusters[endereco_input] = endereco_input
							// fazendo isso libera mais espaço e da para salvar ate 200 mil clusters
							fmt.Println("Endereço: ", endereco, " Indice: ", indice)
							fmt.Println("Tamanho Txs: ", endereco_d1.N_tx)
							cluster.Clusters[endereco_input] = ""
							fmt.Println(" -------- Identificador Cluster: ", hashTransacao,
								" -----")
							fmt.Println(" -------- O endereco: ", endereco_input,
								" foi adicionado no cluster (Lista da endereços) -----")
						}
					}

					identificadorCluster := Model.Identificador{
						Identificador:  hashTransacao,
						TamanhoCluster: len(cluster.Clusters),
					}

					documentsMapCluster = append(documentsMapCluster, cluster)
					documentsIdentificadores = append(documentsIdentificadores, identificadorCluster)

					// Adiciona os valores os HashTransação que serão adicionados
					allIdentificadores[hashTransacao] = ""
					IdentificadoresSeraoSalvos = append(IdentificadoresSeraoSalvos, hashTransacao)
				}
				fmt.Println()
			}
			tamanhoDocsMapCluster := len(documentsMapCluster)
			tamanhoDocsIdentificadores := len(documentsIdentificadores)

			if tamanhoDocsMapCluster != tamanhoDocsIdentificadores {
				fmt.Println("Erro o tamanho de clusters e identificadores nao podem ser diferentes")
				encerraExecucao = true
				break
			}
			fmt.Println("Quantidade de clusters que serão salvos: ", tamanhoDocsIdentificadores)
			if tamanhoDocsIdentificadores > 0 {
				err := Function.EscreverTexto(IdentificadoresSeraoSalvos, hashParaDeletar)
				if err != nil {
					fmt.Println("Erro em escrever os hash no arquivo ", hashParaDeletar)
					panic(err)
				}
				time.Sleep(time.Second * time.Duration(1))
				confirmSalveCluster := Function.SaveMapClusters(documentsMapCluster, ConnectionMongo, DB_Cluster, Collection_Cluster_Distancia2)
				if confirmSalveCluster {
					confirmSalveIdentificadorCluster := Function.SaveIdentificadores(documentsIdentificadores, ConnectionMongo, DB_Cluster, Collection_Cluster_Identificadores_2)
					if confirmSalveIdentificadorCluster {
						// inicia os listas
						IdentificadoresSeraoSalvos = []string{}
						documentsMapCluster = []interface{}{}
						documentsIdentificadores = []interface{}{}

						Function.LimpaTxt(hashParaDeletar)
						fmt.Println(" Salvo com sucesso os cluster e o seus identificadores ")

						fmt.Println("Incrementa indice")
						Function.IncrementaIndice(indice, indiceSaveAddr)
						fmt.Println(" Fim da criação de clusters das transações do endereço: ", endereco)
						fmt.Println()
					} else {
						fmt.Println(" Finaliza Execução: Erro o cluster e o identificador não foram salvos")
						encerraExecucao = true
						break
					}
				} else {
					fmt.Println(" Finaliza Execução, Erro o cluster não foi salvo: ")
					encerraExecucao = true
					break
				}
			} else {
				fmt.Println("Incrementa indice")
				Function.IncrementaIndice(indice, indiceSaveAddr)
				fmt.Println("Não existe uma nova transação do endereço: ", endereco)
			}
		}
		offset = offset + tamanho_txs_endereco_d1
		fmt.Println("Define o offset para ", offset)
		Function.DefineIndice(offset, offsetAddr)
		fmt.Println("Define o indice do endereço para 0")
		Function.DefineIndice(0, indiceSaveAddr)
		fmt.Println()
		fmt.Println("Fim criação de clusters")
	}
}

func TransferindoClusters() {
	Function.TransfereClustersD1_D2(ConnectionMongo, DB_Cluster, DB_Cluster, Collection_Cluster_Identificadores_1, Collection_Cluster_Distancia1, Collection_Cluster_Identificadores_2, Collection_Cluster_Distancia2, DB_Cluster, Col_Cluster_Identificadores, Col_Cluster_Clusters)
}
