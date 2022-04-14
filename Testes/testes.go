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
			Clusters:      map[string]string{"A": "A", "B": "B"},
		}, Model.MapCluster{
			Identificador: "2",
			Clusters:      map[string]string{"C": "C", "B": "B"},
		}, Model.MapCluster{
			Identificador: "3",
			Clusters:      map[string]string{"Q": "Q", "W": "W"},
		}, Model.MapCluster{
			Identificador: "4",
			Clusters:      map[string]string{"R": "R", "T": "T"},
		}, Model.MapCluster{
			Identificador: "5",
			Clusters:      map[string]string{"Q": "Q", "R": "R"},
		}, Model.MapCluster{
			Identificador: "6",
			Clusters:      map[string]string{"A": "A", "Q": "Q"},
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
