package main

import (
	"Tcc/Shared/Function"
	"Tcc/Shared/Model"
	"fmt"
)

var ConnectionMongo string = "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"

//var DB_Cluster string = "Cluster"
var DB_Cluster string = "teste"
var Collection_Cluster string = "Distancia1"
var Collection_Cluster_Map string = "Map_Distancia1"
var Collection_Identificadores string = "Identificadores_1"
var limit int64 = 10000000000
var limit_cluster int = 100000

func main() {
	h1_Map()
}

func h1_Map() {

	for {
		save, err, executeAll := h1_im()
		if err {
			break
		} else if !save {
			break
		} else if executeAll {
			break
		}
	}
}

func h1_im() (save, erro, executeAll bool) {
	clusters := Function.GetAllMapClustersLimit(limit, ConnectionMongo, DB_Cluster, Collection_Cluster_Map)
	if len(clusters) > 0 {
		for indice_cluster, cluster := range clusters {

			identificador_cluster := cluster.Identificador
			map_enderecos := cluster.Clusters
			map_enderecos_resultante := map[string]string{}
			tamanho_map_enderecos := len(map_enderecos)
			indice_map_enderecos := 0
			fmt.Println(" Indice do Cluster: ", indice_cluster)
			fmt.Println(" Identificador do Cluster: ", identificador_cluster)
			for endereco, _ := range map_enderecos {
				fmt.Println(" ---- Tamanho do Mapas de endereços: ", tamanho_map_enderecos)
				fmt.Println(" ---- Indice do Mapas de endereços: ", indice_map_enderecos)
				fmt.Println(" ---- Endereço que esta sendo procurado: ", endereco)
				clusters_que_contem_o_endereco := Function.SearchAddrMapClusters(limit, endereco, identificador_cluster,
					ConnectionMongo, DB_Cluster, Collection_Cluster_Map)
				tamanho_clusters_encontrados := len(clusters_que_contem_o_endereco)

				if tamanho_clusters_encontrados > 0 {
					identificadores := GetIdentificadores(clusters_que_contem_o_endereco)
					CountCluster(identificador_cluster, identificadores, map_enderecos_resultante,
						ConnectionMongo, DB_Cluster, Collection_Cluster_Map)
					tamanho_map_enderecos_resultante := len(map_enderecos_resultante)
					if tamanho_map_enderecos_resultante > limit_cluster {
						// Verificar o tamanho da lista de endereços sem repetição
						// Se for maior 100000 não une as listas de endereços
						// Apaga os identificadores qye nao serao usadas
						// Atualiza os clusters para o identificador novo
						return ProcessCluster_M1(tamanho_map_enderecos_resultante, identificadores, identificador_cluster,
							ConnectionMongo, DB_Cluster, Collection_Cluster_Map, Collection_Identificadores)
					} else {
						// Se for menor une as listas de endereços
						// Apaga os identificadores que nao serão usadas
						// Apaga os clusters que nao serão usadas
						// Atualiza o tamanho do cluster
						return ProcessCluster_M2(map_enderecos_resultante, tamanho_map_enderecos_resultante,
							identificadores, identificador_cluster, ConnectionMongo, DB_Cluster,
							Collection_Cluster_Map, Collection_Identificadores)
					}

					// Criar metodo que sobreescrever o mapa
					// Criar metodo que sobreescrever o identificador
					// Criar metodo que apaga os cluster
					// Criar metodo que apaga os identificadores
				} else {
					fmt.Println(" ---- Somente um cluster contem o endereço: ", endereco)
				}
				indice_map_enderecos++
			}
		}
	} else {
		fmt.Println(" Não existem clusters")
		return false, true, false
	}
	return false, true, false
}

// Conta o tamanho do cluster
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

func GetIdentificadores(clusters_que_contem_o_endereco []Model.MapCluster) (identificadores []string) {
	for _, identificador := range clusters_que_contem_o_endereco {
		identificadores = append(identificadores, identificador.Identificador)
	}
	return identificadores
}

func ProcessCluster_M1(tamanho_map_enderecos_resultante int, identificadores []string, identificadorBase, ConnectionMongo, DB_Cluster,
	Collection_Cluster_Map, Collection_Identificadores string) (save, erro, executeAll bool) {
	var confirm bool

	for _, elem := range identificadores {
		confirm = Function.PutIdentificador(identificadorBase, elem, ConnectionMongo, DB_Cluster, Collection_Cluster_Map)
		if confirm {
			confirm = Function.DeleteIdentificadoresCluster(elem, ConnectionMongo, DB_Cluster, Collection_Identificadores)
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

/* ProcessCluster_M2 Se for menor une as listas de endereços
Apaga os identificadores que nao serão usadas
Apaga os clusters que nao serão usadas
Atualiza o tamanho do cluster */
func ProcessCluster_M2(map_enderecos_resultante map[string]string, tamanho_map_enderecos_resultante int,
	identificadores []string, identificadorBase, ConnectionMongo, DB_Cluster,
	Collection_Cluster_Map, Collection_Identificadores string) (save, erro, executeAll bool) {
	var sucesso bool

	sucesso = Function.PutMapCluster(map_enderecos_resultante, identificadorBase, ConnectionMongo, DB_Cluster, Collection_Cluster_Map)

	if sucesso {
		sucesso = Function.DeleleListIdentificadoresAndClusters(identificadores, ConnectionMongo, DB_Cluster, Collection_Identificadores, Collection_Cluster_Map)
		if !sucesso {
			fmt.Println(" Erro: Não foi Deletado os identificadores e os Clusters")
			return false, true, false
		}

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

func h1() {
	encerraExecucao := false
	clusters := Function.GetAllClustersLimit(limit, ConnectionMongo, DB_Cluster, Collection_Cluster)
	for indice_cluster, cluster := range clusters {
		if encerraExecucao {
			break
		}
		identificador_cluster := cluster.Identificador
		list_enderecos := cluster.Clusters
		tamanho_list_enderecos := len(list_enderecos)
		fmt.Println(" Indice do Cluster: ", indice_cluster)
		fmt.Println(" Identificador do Cluster: ", identificador_cluster)
		for indice_list_enderecos, endereco := range list_enderecos {
			fmt.Println(" ---- Tamanho da lista de endereços: ", tamanho_list_enderecos)
			fmt.Println(" ---- Indice da lista de endereços: ", indice_list_enderecos)
			fmt.Println(" ---- Endereço que esta sendo procurado: ", endereco)
			clusters_contem_o_endereco := Function.SearchAddrClusters(limit, endereco, identificador_cluster,
				ConnectionMongo, DB_Cluster, Collection_Cluster)
			tamanho_clusters_encontrados := len(clusters_contem_o_endereco)
			if tamanho_clusters_encontrados > 0 {
				// Verificar o tamanho da lista de endereços sem repetição
				// Se for maior 100000 não une as listas de endereços
				// Apaga os identificadores qye nao serao usadas
				// Atualiza os clusters para o identificador novo
				// Se for menor une as listas de endereços
				// Apaga os identificadores que nao serão usadas
				// Apaga os clusters que nao serão usadas
			} else {
				fmt.Println(" ---- Somente um cluster contem o endereço: ", endereco)
			}
		}
	}
}
