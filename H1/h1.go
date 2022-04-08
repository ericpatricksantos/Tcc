package main

import (
	"Tcc/Shared/Function"
	"fmt"
)

var ConnectionMongo string = "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
var DB_Cluster string = "Cluster"
var Collection_Cluster string = "Distancia1"
var Collection_Identificadores string = "Identificadores_1"
var limit int64 = 10000000000

func main() {
	h1()
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

func h1_Map() {
	encerraExecucao := false
	clusters := Function.GetAllMapClustersLimit(limit, ConnectionMongo, DB_Cluster, Collection_Cluster)
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
			clusters_contem_o_endereco := Function.SearchAddrMapClusters(limit, endereco, identificador_cluster,
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
