package main

import (
	"Tcc/Shared/Function"
	"Tcc/Shared/Model"
	"fmt"
	"math"
	"strconv"
)

var ConnectionMongo string = "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
var DB_Cluster string = "teste"
var Collection_Identificadores string = "IdenT"
var Collection_Cluster string = "ClusT"

func main() {
	fmt.Println("Teste")
}

func TesteFun() {

	clusters := []interface{}{
		Model.MapCluster{
			Identificador: "B",
			Clusters:      map[string]string{"A": "", "B": ""},
		}, Model.MapCluster{
			Identificador: "C",
			Clusters:      map[string]string{"C": "", "B": ""},
		}, Model.MapCluster{
			Identificador: "B",
			Clusters:      map[string]string{"Q": "", "W": ""},
		}, Model.MapCluster{
			Identificador: "C",
			Clusters:      map[string]string{"R": "", "T": ""},
		}, Model.MapCluster{
			Identificador: "A",
			Clusters:      map[string]string{"Q": "", "R": ""},
		}, Model.MapCluster{
			Identificador: "Z",
			Clusters:      map[string]string{"Q": "", "R": ""},
		},
	}
	iden := []interface{}{
		Model.Identificador{
			Identificador:  "B",
			TamanhoCluster: 1,
		}, Model.Identificador{
			Identificador:  "C",
			TamanhoCluster: 2,
		}, Model.Identificador{
			Identificador:  "B",
			TamanhoCluster: 2,
		}, Model.Identificador{
			Identificador:  "C",
			TamanhoCluster: 3,
		}, Model.Identificador{
			Identificador:  "A",
			TamanhoCluster: 5,
		}, Model.Identificador{
			Identificador:  "Z",
			TamanhoCluster: 5,
		},
	}

	Function.SaveMapClusters(clusters,
		"mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb",
		DB_Cluster, Collection_Cluster)

	Function.SaveMapClusters(iden,
		"mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb",
		DB_Cluster, Collection_Identificadores)

	var confirm bool
	confirm = Function.PutIdentificadores("A", []string{"B", "C", "B", "C"}, ConnectionMongo, DB_Cluster, Collection_Cluster)
	if confirm {
		confirm = Function.DeleteListIdentificadoresCluster([]string{"B", "C", "B", "C"}, ConnectionMongo, DB_Cluster, Collection_Identificadores)
		if confirm {
			fmt.Println(" Atualizado o identificador e deletado os identificadores")
		} else {
			fmt.Println(" Erro: Não foi Atualizado o identificador e deletado os identificadores")
		}
	} else {
		fmt.Println("Erro 22")
	}
}

func l(f map[string]string) {
	delete(f, "a")
}
func testt() {
	colClusters := "clusterTeste"
	colIdentif := "IdentifTeste"
	ConnectionMongo := "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
	DB_Cluster := "teste"

	identificador := Model.Identificador{
		Identificador:  "1jjjAAAAAAAAAAAAAAAAAAAAAAAAAjjjjjjjjjjjjjjjjjjjjjjjjjj",
		TamanhoCluster: 20000000000,
	}
	clusters := map[string]string{}
	for j := 0; j < 2000; j++ {
		ch := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" + strconv.Itoa(j)
		clusters[ch] = ""
	}
	fmt.Println("Cluster: ", len(clusters))

	cluster := Model.MapCluster{
		Identificador: "1jjjAAAAAAAAAAAAAAAAAAAAAAAAAjjjjjjjjjjjjjjjjjjjjjjjjjj",
		Clusters:      clusters,
	}
	fmt.Println("Identificador: ", cluster.Identificador)
	fmt.Println("Tamanho desse Cluster: ", len(cluster.Clusters))
	documentsMapCluster := []interface{}{}
	documentsIdentificadores := []interface{}{}
	for i := 0; i <= 2000; i++ {
		documentsMapCluster = append(documentsMapCluster, cluster)
		documentsIdentificadores = append(documentsIdentificadores, identificador)
	}

	fmt.Println(len(documentsMapCluster))
	fmt.Println(len(documentsIdentificadores))

	Function.SaveMapClusters(documentsMapCluster, ConnectionMongo, DB_Cluster, colClusters)
	Function.SaveIdentificadores(documentsIdentificadores, ConnectionMongo, DB_Cluster, colIdentif)
}

func Teste() {
	Collection_Cluster_Identificadores_1 := "Identificadores_1"
	Collection_Cluster_Identificadores_2 := "Identificadores_2"
	ConnectionMongo := "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
	DB_Cluster := "Cluster"

	allIdentificadores := Function.UnionIdentificadoresD1_D2(ConnectionMongo, DB_Cluster, Collection_Cluster_Identificadores_1, Collection_Cluster_Identificadores_2)

	fmt.Println(len(allIdentificadores))
}

func ParteFracionario(valorReal int, dividendo float32) int {
	valorRealFracionario := float32(valorReal) / dividendo
	valorRealInteiro := valorReal / int(dividendo)
	resultadoParcial := valorRealFracionario - float32(valorRealInteiro)
	resultado := int(resultadoParcial * dividendo)
	return resultado
}

func UltimoValor(valorReal int, dividendo float64) int {
	valor := (float64(valorReal) / dividendo)
	_, valorRealFracionario := math.Modf(float64(valor))
	resultado := int(valorRealFracionario * dividendo)
	return resultado
}

func TestePesquisa() {
	v := Function.SearchAddrClusters(1000, "1JawWE56G5NmnB5iuYbFikbdETs88Fxkwo",
		"16233a85df104fd500c3a1739dd1471a563c36537755be6a7eeeac9c00ff4dbf",
		"mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb",
		"Cluster", "Distancia1")
	f := Function.SearchAddrClusters(1000, "1JawWE56G5NmnB5iuYbFikbdETs88Fxkwo",
		"pp",
		"mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb",
		"Cluster", "Distancia1")

	fmt.Println(v)
	fmt.Println(f)
}

func TesteMap() {
	lista := []string{"1", "1", "1", "1", "2", "2", "2"}
	c := 0
	for {
		if c == 1000 {
			break
		}
		lista = append(lista, strconv.Itoa(c))
		c++
	}
	mapas := map[string]string{}

	for _, i := range lista {
		_, check := mapas[i]
		if check {
			continue
		} else {
			mapas[i] = i
		}
	}
	fmt.Println(mapas)
}

func CreateClusterTeste() {
	clusters := []Model.MapCluster{
		Model.MapCluster{
			Identificador: "1",
			Clusters:      map[string]string{"A": "", "B": ""},
		}, Model.MapCluster{
			Identificador: "2",
			Clusters:      map[string]string{"C": "", "B": ""},
		}, Model.MapCluster{
			Identificador: "3",
			Clusters:      map[string]string{"Q": "", "W": ""},
		}, Model.MapCluster{
			Identificador: "4",
			Clusters:      map[string]string{"R": "", "T": ""},
		}, Model.MapCluster{
			Identificador: "5",
			Clusters:      map[string]string{"Q": "", "R": ""},
		}, Model.MapCluster{
			Identificador: "6",
			Clusters:      map[string]string{"A": "", "Q": ""},
		},
	}

	for _, cluster := range clusters {
		Function.SalvaMapCluster(cluster,
			"mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb",
			"teste", "Map_Distancia1")
	}
}

func QuantidadeEnderecos() {
	clusters := Function.GetAllMapClustersLimit(1000000,
		"mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb",
		"teste", "Map_Distancia1")
	enderecos := map[string]string{}

	for _, cluster := range clusters {
		for _, v := range cluster.Clusters {
			_, ok := enderecos[v]
			if ok {
				continue
			} else {
				enderecos[v] = v
			}
		}
	}
	// 7 enderecos diferentes
	fmt.Println(enderecos)
	fmt.Println(len(enderecos))
}

func TesteCriacao() {
	v := map[string]string{}
	for i := 0; i < 1; i++ {
		v["AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"+strconv.Itoa(i)] = ""
	}

	conf := Function.PutMapCluster(v, "1",
		"mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb",
		"teste", "Map_Distancia1")
	if conf {
		fmt.Println("deu bom")
	}
}

func TamUnicoCluster() {
	clusters := Function.GetAllMapClustersLimit(1000000,
		"mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb",
		"teste", "Map_Distancia1")

	x := len(clusters[0].Clusters)
	fmt.Println(x)
}

func Correcao() {
	// indice do cluster = 1162
	// offset = 190000
	//identificador = ccea8c5237e5cc07c870d3e3c18104492f858804a6e722cbfea11091f2c9fd49
	tamanho_identificador_base := Function.GetIdentificadorById("ccea8c5237e5cc07c870d3e3c18104492f858804a6e722cbfea11091f2c9fd49", "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb",
		"Cluster", "Identificadores")

	fmt.Println(tamanho_identificador_base)
	identificador := []string{"ccea8c5237e5cc07c870d3e3c18104492f858804a6e722cbfea11091f2c9fd49"}

	clusters := Function.SearchAddrMapsClusters(1000000, identificador, "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb",
		"Cluster", "Clusters")
	tamanho_cluster := map[string]string{}
	for _, item := range clusters {
		for endereco, _ := range item.Clusters {
			_, ok := tamanho_cluster[endereco]
			if ok {
				continue
			} else {
				tamanho_cluster[endereco] = ""
			}
		}
	}
	g := len(tamanho_cluster)
	fmt.Println(g)

	confirm := Function.PutTamanhoCluster(g, "ccea8c5237e5cc07c870d3e3c18104492f858804a6e722cbfea11091f2c9fd49",
		"mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb",
		"Cluster", "Identificadores")
	if confirm {
		fmt.Println("certo")
	}

	tamanho_identificador_base = Function.GetIdentificadorById("ccea8c5237e5cc07c870d3e3c18104492f858804a6e722cbfea11091f2c9fd49", "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb",
		"Cluster", "Identificadores")

	fmt.Println(tamanho_identificador_base)
}
