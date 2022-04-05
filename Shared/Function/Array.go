package Function

import (
	"fmt"
	"Tcc/Shared/Model"
)

func Contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func AuxH1(cluster1 []string, cluster2 []string) (bool, []string) {
	for _, item := range cluster1 {
		confirm := Contains(cluster2, item)

		if confirm {
			return true, UnionArray(cluster1, cluster2)
		}
	}

	return false, []string{}
}

func RemoveDuplicados(lista []string) ([]string, int) {
	var temp []string

	for _, x := range lista {
		if !Contains(temp, x) && len(x) > 0 {
			temp = append(temp, x)
		}
	}

	return temp, len(temp)
}

func RemoveItem(lista []string, item string) ([]string, int) {
	var temp []string

	for _, x := range lista {
		if !Contains(temp, x) && len(x) > 0 && x != item {
			temp = append(temp, x)
		}
	}

	return temp, len(temp)
}

func UnionArray(input []string, out []string) []string {
	return append(input, out...)
}

func TesteRemoveCluster() {
	lista := []string{}
	for {
		if len(lista) == 600 {
			break
		}
		lista = append(lista, "a")
	}
	matriz, _ := TransformaArrayEmMatriz(lista, 0, 300)
	ma := RemoveMatrizVazia(matriz)
	fmt.Println(ma)
}

func RemoveCluster(hash string, clusters []Model.Cluster) (result []Model.Cluster) {
	if hash == "" {
		return result
	} else if clusters == nil {
		return result
	} else if len(hash) == 0 {
		return result
	} else if len(clusters) == 0 {
		return result
	} else {
		for _, item := range clusters {
			if item.Hash != hash {
				result = append(result, item)
			}
		}

		return result
	}
}

func RemoveMatrizVazia(matrizes [][]string) [][]string {
	result := [][]string{}
	for _, matriz := range matrizes {
		if len(matriz) > 0 {
			result = append(result, matriz)
		}
	}
	return result
}

func TransformaArrayEmMatriz(listaEnderecos []string, indiceInicial int, limiteEnderecos int) ([][]string, int) {
	QtdLinhas := (len(listaEnderecos) / 300) + 1
	limite := limiteEnderecos //Qtd de Colunas na Matriz
	matrizEnderecos := make([][]string, QtdLinhas)
	if indiceInicial > 0 {
		//Remove os elementos que foram salvos na matriz
		listaEnderecos = append(listaEnderecos[:0], listaEnderecos[indiceInicial:]...)

	}

	for contador := 0; contador < QtdLinhas; contador++ {

		// Recebe uma fatia do array baseado no LimiteEnderecos
		enderecosSeparados := listaEnderecos[:limite]
		if len(enderecosSeparados) == 0 {
			break
		}
		//Alocar memoria para o array de string(Quantidade de colunas)
		matrizEnderecos[contador] = make([]string, len(enderecosSeparados))
		for j := 0; j < len(enderecosSeparados); j++ {
			//Atribuir os valores do slice para a matriz
			matrizEnderecos[contador][j] = enderecosSeparados[j]
		}

		//Remove os elementos que foram salvos na matriz
		listaEnderecos = append(listaEnderecos[:0], listaEnderecos[limite:]...)

		// Se o tamanho do slice for menor do que o tamanho maximo do Array,
		// setar tamanhoMaximo = tamanho Array
		// Evitar acesso invalido de memoria
		if limite > (len(listaEnderecos)) {
			limite = len(listaEnderecos)
		}

	}

	return matrizEnderecos, QtdLinhas
}

func GetHash(cluster []Model.Cluster) (valores []string) {
	for _, item := range cluster {
		valores = append(valores, item.Hash)
	}
	return valores
}

func EliminaElem(analisado []string, input []string) (result []string, tamanho int) {
	for _, item := range analisado {
		if !Contains(input, item) && len(item) > 0 {
			result = append(result, item)
		}
	}
	return result, len(result)
}
