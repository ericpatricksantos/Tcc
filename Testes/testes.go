package main

import (
	"Tcc/Shared/Function"
	"fmt"
	"math"
	"strconv"
)

func main() {

	clusters_contem_o_endereco := Function.SearchAddrMapClusters(1000000, "1JawWE56G5NmnB5iuYbFikbdETs88Fxkwo",
		"16233a85df104fd500c3ak1739dd1471a563c36537755be6a7eeeac9c00ff4dbf",
		"mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb",
		"Cluster", "Map_Distancia1")

	fmt.Println(len(clusters_contem_o_endereco))
	// ultimoValor := (1512 / 1000.0)
	//x, o := math.Modf(ultimoValor)
	//fmt.Println(x)
	//fmt.Println(o)
	//
	// fmt.Println("Funcao: ", UltimoValor(1511, 1000.0))
	//
	//fmt.Println("Funcao: ", ParteFracionario(1511, 1000.0))
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
