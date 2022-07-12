package main

import (
	"Tcc/Shared/Function"
	"Tcc/Shared/Model"
	"fmt"
	"time"
)

var applicationStateFile = "./ApplicationStateFile/"
var ConnectionMongo string
var DB_Cluster string
var Collection_Cluster_Map string
var Collection_Identificadores string
var limit int64 = 2000
var limit_search int64 = 10000
var limit_cluster int = 100000
var pausa int = 100
var offsetClusters string
var indiceCluster string

func init() {
	offsetClusters = applicationStateFile + "offsetClusters.txt" // Arquivo usado para pula os clusters processados
	indiceCluster = applicationStateFile + "IndiceCluster.txt"   // Arquivo usado para salvar o indice do cluster que esta sendo processado
	ConnectionMongo = "mongodb://127.0.0.1:27017/"
	DB_Cluster = "Testando"                        // Database do Cluster
	Collection_Cluster_Map = "Clusters"            // Collection que contem os clusters
	Collection_Identificadores = "Identificadores" // Collection que contem os identificadores do clusters

	fmt.Println("Criando os arquivos da aplicação")
	Function.CreateListFile([]string{offsetClusters, indiceCluster})
}

func main() {
	AlgorithmH1()
}

func AlgorithmH1() {

	enderecos_repetem := GetAddrsRepeat()
	for {
		tam := len(enderecos_repetem)
		fmt.Println("Quantidade de endereços que repetem: ", tam)
		if tam == 0 {
			break
		}
		pausa = 100
		offset := Function.BuscaIndice(offsetClusters)
		save, err, executeAll := h1(enderecos_repetem, offset)
		if err {
			break
		} else if !save {
			break
		} else if executeAll {
			break
		}
	}
}

func h1(enderecos_repetem map[string]string, offset int) (save, erro, executeAll bool) {
	clusters := Function.GetAllMapClustersLimitOffset(limit, int64(offset), ConnectionMongo, DB_Cluster, Collection_Cluster_Map)
	tamanho_clusters := len(clusters)

	if tamanho_clusters > 0 {
		indice_cluster_inicial := Function.BuscaIndice(indiceCluster)
		if indice_cluster_inicial >= tamanho_clusters {
			indice_cluster_inicial = 0
			Function.DefineIndice(0, indiceCluster)
		}
		for indice_cluster := indice_cluster_inicial; indice_cluster < tamanho_clusters; indice_cluster++ {

			identificador_cluster := clusters[indice_cluster].Identificador
			map_enderecos := clusters[indice_cluster].Clusters
			map_enderecos_resultante := map[string]string{}
			tamanho_map_enderecos := len(map_enderecos)
			indice_map_enderecos := 0
			fmt.Println(" Indice do Cluster: ", indice_cluster)
			fmt.Println(" Identificador do Cluster: ", identificador_cluster)
			for endereco, _ := range map_enderecos {
				fmt.Println(" ---- Tamanho do Mapas de endereços: ", tamanho_map_enderecos)
				fmt.Println(" ---- Indice do Mapas de endereços: ", indice_map_enderecos)
				fmt.Println(" ---- Endereço que esta sendo procurado: ", endereco)

				_, ok := enderecos_repetem[endereco]
				if ok {
					clusters_que_contem_o_endereco := Function.AllSearchAddrMapClusters(limit_search, endereco, identificador_cluster,
						ConnectionMongo, DB_Cluster, Collection_Cluster_Map)
					tamanho_clusters_encontrados := len(clusters_que_contem_o_endereco)

					if tamanho_clusters_encontrados > 0 {
						identificadores := GetIdentificadores(clusters_que_contem_o_endereco)
						CountCluster(identificador_cluster, identificadores, map_enderecos_resultante,
							ConnectionMongo, DB_Cluster, Collection_Cluster_Map)
						tamanho_map_enderecos_resultante := len(map_enderecos_resultante)
						fmt.Println(" ---- Tamanho do Mapa Resultante: ", tamanho_map_enderecos_resultante)
						if tamanho_map_enderecos_resultante > limit_cluster {
							// Verificar o tamanho da lista de endereços sem repetição
							// Se for maior 100000 não une as listas de endereços
							// Apaga os identificadores qye nao serao usadas
							// Atualiza os clusters para o identificador novo
							save, erro, executeAll := ProcessCluster_M1(tamanho_map_enderecos_resultante, identificadores, identificador_cluster,
								ConnectionMongo, DB_Cluster, Collection_Cluster_Map, Collection_Identificadores)
							if save {
								delete(enderecos_repetem, endereco)
							}
							return save, erro, executeAll
						} else {
							// Se for menor une as listas de endereços
							// Apaga os identificadores que nao serão usadas
							// Apaga os clusters que nao serão usadas
							// Atualiza o tamanho do cluster
							save, erro, executeAll := ProcessCluster_M2(map_enderecos, map_enderecos_resultante, tamanho_map_enderecos_resultante,
								identificadores, identificador_cluster, ConnectionMongo, DB_Cluster,
								Collection_Cluster_Map, Collection_Identificadores)
							if save {
								delete(enderecos_repetem, endereco)
							}
							return save, erro, executeAll
						}
					} else {
						delete(enderecos_repetem, endereco)
						fmt.Println(" ---- Somente um cluster contem o endereço: ", endereco)
					}

					if indice_map_enderecos == pausa {
						time.Sleep(time.Second * time.Duration(1))
						pausa = pausa + 100
					}
				} else {
					fmt.Println(" O endereço ", endereco, " nao repete")
				}
				indice_map_enderecos++
			}
			Function.IncrementaIndice(indice_cluster, indiceCluster)
		}

		offset = offset + tamanho_clusters
		Function.DefineIndice(offset, offsetClusters)
	} else {
		fmt.Println(" Não existem clusters")
		fmt.Println("Definindo offset para zero")
		Function.DefineIndice(0, offsetClusters)
		return true, false, false
	}
	fmt.Println("-- FIM --")
	return true, false, false
}

/* ProcessCluster_M1
Apaga os identificadores qye nao serao usadas
Atualiza os clusters para o identificador novo
Atualiza o tamanho do cluster
*/
func ProcessCluster_M1(tamanho_map_enderecos_resultante int, identificadores []string, identificadorBase, ConnectionMongo, DB_Cluster,
	Collection_Cluster_Map, Collection_Identificadores string) (save, erro, executeAll bool) {
	var confirm bool
	confirm = Function.PutIdentificadores(identificadorBase, identificadores, ConnectionMongo, DB_Cluster, Collection_Cluster_Map)
	if confirm {
		confirm = Function.DeleteListIdentificadoresCluster(identificadores, ConnectionMongo, DB_Cluster, Collection_Identificadores)
		if confirm {
			fmt.Println(" Atualizado o identificador e deletado os identificadores")
		} else {
			fmt.Println(" Erro: Não foi Atualizado o identificador e deletado os identificadores")
			return false, true, false
		}
	} else {
		fmt.Println(" Erro: Não foi Atualizado os identificadores: ", identificadores, " para o identificador: ", identificadorBase)
		return false, true, false
	}

	tamanho_identificador_base := Function.GetIdentificadorById(identificadorBase, ConnectionMongo, DB_Cluster, Collection_Identificadores)

	if len(tamanho_identificador_base.Identificador) == 0 || tamanho_identificador_base.TamanhoCluster == 0 {
		fmt.Println(" Erro ao buscar pelo identificador base")
		return false, true, false
	}

	if tamanho_map_enderecos_resultante > tamanho_identificador_base.TamanhoCluster {
		confirm = Function.PutTamanhoCluster(tamanho_map_enderecos_resultante, identificadorBase, ConnectionMongo, DB_Cluster, Collection_Identificadores)
		if confirm {
			fmt.Println(" Tamanho do cluster do identificador base: ", identificadorBase, " atualizado para ", tamanho_map_enderecos_resultante)
			return true, false, false
		} else {
			fmt.Println(" Erro: Tamanho do cluster do identificador base: ", identificadorBase, " nao foi atualizado para ", tamanho_map_enderecos_resultante)
			return false, true, false
		}
	} else {
		fmt.Println(" Tamanho do Cluster é menor ou igual ao cluster resultante")
		fmt.Println(" Tamanho do Cluster: ", tamanho_identificador_base.TamanhoCluster, " Tamanho Cluster Resultante: ", tamanho_map_enderecos_resultante)
		return true, false, false
	}

}

/* ProcessCluster_M2
Une as listas de endereços
Apaga os identificadores que nao serão usadas
Apaga os clusters que nao serão usadas
Atualiza o tamanho do cluster */
func ProcessCluster_M2(map_endereco_atual, map_enderecos_resultante map[string]string, tamanho_map_enderecos_resultante int,
	identificadores []string, identificadorBase, ConnectionMongo, DB_Cluster,
	Collection_Cluster_Map, Collection_Identificadores string) (save, erro, executeAll bool) {
	var sucesso bool

	// verifica se o map_endereco_atual e o map_enderecos_resultante são iguais
	if Function.VerificaIgualdade(map_endereco_atual, map_enderecos_resultante) {
		fmt.Println("----------- O cluster atual é igual o cluster resultante, por isso nao precisa atualizar, somente apagar os identificadores e o clusters")
		sucesso = true
	} else {
		fmt.Println("----------- O cluster atual é diferente do cluster resultante ")
		fmt.Println("Atualizando o cluster do identificadorBase")
		sucesso = Function.PutMapCluster(map_enderecos_resultante, identificadorBase, ConnectionMongo, DB_Cluster, Collection_Cluster_Map)
	}

	if sucesso {
		fmt.Println("Deletando os outros identificadores")
		sucesso = Function.DeleteListIdentificadoresAndClusters(identificadores, ConnectionMongo, DB_Cluster, Collection_Identificadores, Collection_Cluster_Map)
		if !sucesso {
			fmt.Println(" Erro: Não foi Deletado os identificadores e os Clusters")
			return false, true, false
		}

		fmt.Println("Buscando tamanho do identificadorBase")
		tamanho_identificador_base := Function.GetIdentificadorById(identificadorBase, ConnectionMongo, DB_Cluster, Collection_Identificadores)

		if len(tamanho_identificador_base.Identificador) == 0 || tamanho_identificador_base.TamanhoCluster == 0 {
			fmt.Println(" Erro ao buscar pelo identificador base")
			return false, true, false
		}

		if tamanho_map_enderecos_resultante > tamanho_identificador_base.TamanhoCluster {
			sucesso = Function.PutTamanhoCluster(tamanho_map_enderecos_resultante, identificadorBase, ConnectionMongo, DB_Cluster, Collection_Identificadores)
			if sucesso {
				fmt.Println(" Tamanho do cluster do identificador base: ", identificadorBase, " atualizado para ", tamanho_map_enderecos_resultante)
				return true, false, false
			} else {
				fmt.Println(" Erro: Tamanho do cluster do identificador base: ", identificadorBase, " nao foi atualizado para ", tamanho_map_enderecos_resultante)
				return false, true, false
			}
		} else {
			fmt.Println(" Tamanho do Cluster é menor ou igual ao cluster resultante")
			fmt.Println(" Tamanho do Cluster: ", tamanho_identificador_base.TamanhoCluster, " Tamanho Cluster Resultante: ", tamanho_map_enderecos_resultante)
			return true, false, false
		}
	} else {
		fmt.Println(" Erro ao Atualizar o identificador: ", identificadorBase, " com o mapa resultante")
		return false, true, false
	}

}

/* GetAddrsRepeat busca os endereços que estão repetidos*/
func GetAddrsRepeat() map[string]string {
	enderecos := map[string]Model.AddrsRepeat{}
	result := map[string]string{}
	limit := 2000
	offset := 0
	for {
		clusters := Function.GetAllMapClustersLimitOffset(int64(limit), int64(offset), ConnectionMongo, DB_Cluster, Collection_Cluster_Map)

		tam := len(clusters)
		if tam == 0 {
			offset = 0
			break
		}

		for _, cluster := range clusters {
			for ch, _ := range cluster.Clusters {
				_, ok := enderecos[ch]
				if ok {
					if enderecos[ch].Qtd == 1 && enderecos[ch].Identificador != cluster.Identificador {
						qtd := enderecos[ch].Qtd + 1
						enderecos[ch] = Model.AddrsRepeat{
							qtd,
							"+",
						}
						result[ch] = ""
					}
				} else {
					enderecos[ch] = Model.AddrsRepeat{
						1,
						cluster.Identificador,
					}
				}
			}
		}

		offset = offset + tam
	}
	return result
}

/* CountCluster Conta o tamanho do cluster*/
func CountCluster(identificador_cluster string, identificadores []string, mapaAtual map[string]string, ConnectionMongo, DB_Cluster, Collection_Cluster_Map string) {
	identificadoresSeraoBuscados := append([]string{identificador_cluster}, identificadores...)
	// Contem todos os clusters
	clusterResultante := Function.SearchAddrMapsClusters(100000000, identificadoresSeraoBuscados, ConnectionMongo, DB_Cluster, Collection_Cluster_Map)

	for _, cluster := range clusterResultante {
		for endereco, _ := range cluster.Clusters {
			_, ok := mapaAtual[endereco]
			if ok {
				continue
			} else {
				mapaAtual[endereco] = ""
			}
		}
	}
}

/* GetIdentificadores transforma os identificadores dos clusters em uma lista de identificadores */
func GetIdentificadores(clusters_que_contem_o_endereco []Model.MapCluster) (identificadores []string) {
	evitar_repeticao := map[string]string{}
	for _, identificador := range clusters_que_contem_o_endereco {
		_, ok := evitar_repeticao[identificador.Identificador]
		if ok {
			continue
		} else {
			evitar_repeticao[identificador.Identificador] = ""
			identificadores = append(identificadores, identificador.Identificador)
		}
	}
	return identificadores
}
