package main

import (
	"Tcc/Shared/Function"
	"Tcc/Shared/Model"
	"fmt"
	"time"
)

var limit int64 = 100

//var allAddr int = 4

var ConnectionMongo string = "mongodb://127.0.0.1:27017/"

//var ConnectionMongo string = "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
var DB_Endereco string = "Endereco"

//var DB_Endereco string = "teste"

var DB_Cluster string = "Cluster"

//var DB_Cluster string = "teste"

func main() {
	//ContagemEnderecosTotais()
	VerificaEnderecosTotais()
	//CreateClustersDistancia2_Map()
}

// em uso
func CreateClustersDistancia1_Map() {
	encerraExecucao := false
	Collection_Distancia1 := "Farao"
	Collection_Cluster_Identificadores := "Identificadores_1"
	Collection_Cluster_Distancia1 := "Distancia1"
	// as transações dos endereços do faráo estão na distancia 1
	txs_endereco_farao := Function.GetAllAddr(ConnectionMongo, DB_Endereco, Collection_Distancia1)

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
						// cluster.Clusters[endereco_input] = endereco_input
						// fazendo isso libera mais espaço e da para salvar ate 200 mil clusters
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

// em uso
func VerificaCreateClustersDistancia1_Map() {
	colDist1 := "Distancia1"
	clusters := Function.GetAllMapClustersLimit(100000, ConnectionMongo, DB_Cluster, colDist1)
	mapas := map[string]string{}

	for _, cluster := range clusters {
		for endereco, _ := range cluster.Clusters {
			_, ok := mapas[endereco]
			if ok {
				continue
			} else {
				mapas[endereco] = endereco
			}
		}
	}

	if len(mapas) == 6528 {
		fmt.Println(" Correto")
	}
}

// Em uso
func CreateClustersDistancia2_Map() {

	hashDel, _ := Function.LerTexto("hashParaDeletar.txt")

	if len(hashDel) > 0 {
		panic("Existem valores para apagar")
	}

	encerraExecucao := false
	Collection_Distancia1 := "Distancia1"
	//Collection_Distancia1 := "endereco_distancia1" // simula a collection distancia1 do database Endereco
	Collection_Cluster_Identificadores_1 := "Identificadores_1"
	Collection_Cluster_Identificadores_2 := "Identificadores_2"
	Collection_Cluster_Distancia2 := "Distancia2"

	allIdentificadores := Function.UnionIdentificadoresD1_D2(ConnectionMongo, DB_Cluster, Collection_Cluster_Identificadores_1, Collection_Cluster_Identificadores_2)
	fmt.Println("Tamanho de todos os identificadores: ", len(allIdentificadores))
	for {
		offset := Function.BuscaIndice("offsetAddr.txt")
		indiceInicial := Function.BuscaIndice("indiceSaveAddr.txt")
		if encerraExecucao {
			fmt.Println(" Erro ")
			break
		}
		// as transações dos endereços do faráo estão na distancia 1
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
				err := Function.EscreverTexto(IdentificadoresSeraoSalvos, "hashParaDeletar.txt")
				if err != nil {
					fmt.Println("Erro em escrever os hash no arquivo hashParaDeletar.txt")
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

						Function.LimpaTxt("hashParaDeletar.txt")
						fmt.Println(" Salvo com sucesso os cluster e o seus identificadores ")

						fmt.Println("Incrementa indice")
						Function.IncrementaIndice(indice, "indiceSaveAddr.txt")
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
				Function.IncrementaIndice(indice, "indiceSaveAddr.txt")
				fmt.Println("Não existe uma nova transação do endereço: ", endereco)
			}
		}
		offset = offset + tamanho_txs_endereco_d1
		fmt.Println("Define o offset para ", offset)
		Function.DefineIndice(offset, "offsetAddr.txt")
		fmt.Println("Define o indice do endereço para 0")
		Function.DefineIndice(0, "indiceSaveAddr.txt")
		fmt.Println()
		fmt.Println("Fim criação de clusters")
	}
}

func VerificaQtdTransacoesTotais() {
	v := Function.UnionHashTransacaoD1_D2(ConnectionMongo, DB_Endereco, "Farao", "Distancia1")
	// Distancia 1 - 278 transações/clusters
	// Distancia 2 - 633314 transações/clusters

	// Total 633592 transações/clusters
	fmt.Println(len(v))
}

func VerificaEnderecosTotais() {
	v := Function.UnionEnderecosD1_D2(ConnectionMongo, DB_Endereco, "Farao", "Distancia1")
	// Distancia 1 -  6528 endereços
	// Distancia 2 - 1524003 endereços

	// Total 1530531 endereços
	fmt.Println(len(v))
}

func ContagemEnderecosTotais() {
	r := Function.ContagemEnderecosTotais(ConnectionMongo, DB_Cluster, "Distancia1", "Distancia2")

	// Total 1530531 endereços distancia 1 e 2
	fmt.Println(len(r))
}

func CreateClustersDistancia1() {
	encerraExecucao := false
	Collection_Distancia1 := "Farao"
	Collection_Cluster_Identificadores := "Identificadores_1"
	Collection_Cluster_Distancia1 := "Distancia1"
	// as transações dos endereços do faráo estão na distancia 1
	txs_endereco_farao := Function.GetAllAddr(ConnectionMongo, DB_Endereco, Collection_Distancia1)

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
				cluster := Model.Clusters{
					Identificador: hashTransacao,
					Clusters:      []string{},
				}
				for _, input := range tx.Inputs {
					endereco_input := input.Prev_Out.Addr
					existeEndereco := Function.Contains(cluster.Clusters, endereco_input)
					if existeEndereco {
						fmt.Println(" -------- O endereco: ", endereco_input,
							" existe no cluster (Lista da endereços) -----")
					} else {
						cluster.Clusters = append(cluster.Clusters, endereco_input)
						fmt.Println(" -------- O endereco: ", endereco_input,
							" foi adicionado no cluster (Lista da endereços) -----")
					}
				}

				identificadorCluster := Model.Identificador{
					Identificador:  hashTransacao,
					TamanhoCluster: len(cluster.Clusters),
				}

				confirmSalveCluster := Function.SalvaCluster(cluster, ConnectionMongo, DB_Cluster, Collection_Cluster_Distancia1)
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

func CreateClustersDistancia2() {
	encerraExecucao := false
	Collection_Distancia1 := "Distancia"
	Collection_Cluster_Identificadores_1 := "Identificadores_1"
	Collection_Cluster_Identificadores_2 := "Identificadores_2"
	Collection_Cluster_Distancia2 := "Distancia2"
	// as transações dos endereços do faráo estão na distancia 1
	txs_endereco_farao := Function.GetAllAddr(ConnectionMongo, DB_Endereco, Collection_Distancia1)

	for _, endereco_farao := range txs_endereco_farao {
		if encerraExecucao {
			break
		}

		for _, tx := range endereco_farao.Txs {
			if encerraExecucao {
				break
			}

			hashTransacao := tx.Hash
			existeCluster_1 := Function.CheckCluster(ConnectionMongo, DB_Cluster,
				Collection_Cluster_Identificadores_1, "identificador", hashTransacao)
			existeCluster_2 := Function.CheckCluster(ConnectionMongo, DB_Cluster,
				Collection_Cluster_Identificadores_2, "identificador", hashTransacao)
			if existeCluster_1 || existeCluster_2 {
				fmt.Println(" Existe um cluster com essa transação: ", hashTransacao)
				fmt.Println(" Passa para a próxima transação")
			} else {
				cluster := Model.Clusters{
					Identificador: hashTransacao,
					Clusters:      []string{},
				}
				for _, input := range tx.Inputs {
					endereco_input := input.Prev_Out.Addr
					existeEndereco := Function.Contains(cluster.Clusters, endereco_input)
					if existeEndereco {
						fmt.Println(" -------- O endereco: ", endereco_input,
							" existe no cluster (Lista da endereços) -----")
					} else {
						cluster.Clusters = append(cluster.Clusters, endereco_input)
						fmt.Println(" -------- O endereco: ", endereco_input,
							" foi adicionado no cluster (Lista da endereços) -----")
					}
				}

				identificadorCluster := Model.Identificador{
					Identificador:  hashTransacao,
					TamanhoCluster: len(cluster.Clusters),
				}

				confirmSalveCluster := Function.SalvaCluster(cluster, ConnectionMongo, DB_Cluster, Collection_Cluster_Distancia2)
				if confirmSalveCluster {
					confirmSalveIdentificadorCluster := Function.SalvaIdentificadorCluster(identificadorCluster, ConnectionMongo, DB_Cluster, Collection_Cluster_Identificadores_2)
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

func VerificaQtdEnderecosDistancia1() {
	colDist1 := "Distancia1"
	clusters := Function.GetAllClustersLimit(100000, ConnectionMongo, DB_Cluster, colDist1)
	lista := []string{}

	for _, cluster := range clusters {
		for _, endereco := range cluster.Clusters {
			check := Function.Contains(lista, endereco)
			if check {
				continue
			} else {
				lista = append(lista, endereco)
			}
		}
	}
	if len(lista) == 6528 {
		fmt.Println(" Correto")
	}

}
