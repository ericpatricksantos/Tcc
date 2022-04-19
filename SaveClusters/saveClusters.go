package main

import (
	"Tcc/Shared/Function"
	"Tcc/Shared/Model"
	"fmt"
)

var ConnectionMongo string = "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
var DB_Endereco string = "Endereco"
var DB_Cluster string = "Cluster"

func main() {
	VerificaCreateClustersDistancia1_Map()
}

func CreateClustersDistancia1_Map() {
	encerraExecucao := false
	Collection_Distancia1 := "Farao"
	Collection_Cluster_Identificadores := "Map_Identificadores_1"
	Collection_Cluster_Distancia1 := "Map_Distancia1"
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

func VerificaCreateClustersDistancia1_Map() {
	colDist1 := "Map_Distancia1"
	clusters := Function.GetAllMapClustersLimit(100000, ConnectionMongo, DB_Cluster, colDist1)
	mapas := map[string]string{}

	for _, cluster := range clusters {
		for _, endereco := range cluster.Clusters {
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

// em uso
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
			if existeCluster_1 && existeCluster_2 {
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

// em uso
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
