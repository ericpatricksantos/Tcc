package main

import (
	"Tcc/Shared/Function"
	"Tcc/Shared/Model"
	"fmt"
	"math"
	"strconv"
)

func main() {
	CreateClusterTeste()
	//TesteCriacao()
	//TamUnicoCluster()
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
