package Function

import (
	"Tcc/Shared/Model"
	"bufio"
	"fmt"
	"os"
	"strconv"
)

func ExistsFile(caminho string) bool {
	_, erro := os.Stat(caminho)
	if erro != nil {
		return false
	}
	return true
}

func CreateFile(caminho string) bool {
	if ExistsFile(caminho) {
		return true
	} else {
		arquivo, err := os.Create(caminho)
		if err != nil {
			panic(err)
		}
		defer arquivo.Close()
		return true
	}
}

func CreateListFile(caminhos []string) bool {
	for _, item := range caminhos {
		confirm := CreateFile(item)
		if !confirm {
			return confirm
		}
	}
	return true
}

// LerTexto Funcao que le o conteudo do arquivo e retorna um slice the string com todas as linhas do arquivo
func LerTexto(caminhoDoArquivo string) ([]string, error) {
	// Abre o arquivo
	arquivo, err := os.Open(caminhoDoArquivo)
	// Caso tenha encontrado algum erro ao tentar abrir o arquivo retorne o erro encontrado
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Open  que esta sendo chamada na Função LerTexto - {Function/File.go}")
		fmt.Println()

		panic(err)
	}
	// Garante que o arquivo sera fechado apos o uso
	defer arquivo.Close()

	// Cria um scanner que le cada linha do arquivo
	var linhas []string
	scanner := bufio.NewScanner(arquivo)
	for scanner.Scan() {
		linhas = append(linhas, scanner.Text())
	}

	// Retorna as linhas lidas e um erro se ocorrer algum erro no scanner
	return linhas, scanner.Err()
}

// EscreverTexto Funcao que escreve um texto no arquivo e retorna um erro caso tenha algum problema
func EscreverTexto(linhas []string, caminhoDoArquivo string) error {
	// Cria o arquivo de texto
	arquivo, err := os.Create(caminhoDoArquivo)
	// Caso tenha encontrado algum erro retornar ele
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função Create  que esta sendo chamada na Função EscreverTexto - {Function/File.go}")
		fmt.Println()

		panic(err)
	}
	// Garante que o arquivo sera fechado apos o uso
	defer arquivo.Close()

	// Cria um escritor responsavel por escrever cada linha do slice no arquivo de texto
	escritor := bufio.NewWriter(arquivo)
	for _, linha := range linhas {
		fmt.Fprintln(escritor, linha)
	}

	// Caso a funcao flush retorne um erro ele sera retornado aqui tambem
	return escritor.Flush()
}

func EscreverTextoSemApagar(linhas []string, caminhoDoArquivo string) error {
	valoresAntigos, er := LerTexto(caminhoDoArquivo)

	if er != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função LerTexto  que esta sendo chamada na Função EscreverTextoSemApagar - {Function/File.go}")
		fmt.Println()

		panic(er)
	}
	valoresAtual := append(valoresAntigos, linhas...)
	// Cria o arquivo de texto
	arquivo, err := os.Create(caminhoDoArquivo)
	// Caso tenha encontrado algum erro retornar ele
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Create  que esta sendo chamada na Função EscreverTextoSemApagar - {Function/File.go}")
		fmt.Println()

		panic(err)
	}
	// Garante que o arquivo sera fechado apos o uso
	defer arquivo.Close()

	// Cria um escritor responsavel por escrever cada linha do slice no arquivo de texto
	escritor := bufio.NewWriter(arquivo)
	for _, linha := range valoresAtual {
		fmt.Fprintln(escritor, linha)
	}

	// Caso a funcao flush retorne um erro ele sera retornado aqui tambem
	return escritor.Flush()
}

func GetIndiceLogIndice(nomeArquivoIndice string) int {
	valorLogIndice, err := LerTexto(nomeArquivoIndice)
	indiceInicial := 0
	if len(valorLogIndice) > 0 {
		if err != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função LerTexto  que esta sendo chamada na Função GetIndiceLogIndice - {Function/File.go}")
			fmt.Println()

			panic(err)
		}
		var er error
		indiceInicial, er = strconv.Atoi(valorLogIndice[0])
		if er != nil {
			fmt.Println()
			fmt.Println("Erro na resposta da função Atoi  que esta sendo chamada na Função GetIndiceLogIndice - {Function/File.go}")
			fmt.Println()

			panic(err)
		}

	}

	return indiceInicial
}
func BuscaIndice(txtIndice string) (indice int) {
	indice = GetIndiceLogIndice(txtIndice)
	return indice
}

func BuscaDoisIndices(txtIndice1, txtIndice2 string) (indice1, indice2 int) {
	indice1 = GetIndiceLogIndice(txtIndice1)
	indice2 = GetIndiceLogIndice(txtIndice2)

	return indice1, indice2
}

func LimpaTxt(txt string) {
	EscreverTexto([]string{}, txt)
}

func IncrementaIndice(valorAnterior int, txt string) {
	temp := []string{strconv.Itoa(valorAnterior + 1)}
	EscreverTexto(temp, txt)
}

func DefineIndice(valorAtual int, txt string) {
	temp := []string{strconv.Itoa(valorAtual)}
	EscreverTexto(temp, txt)
}

func SalvarListaCluster(clusters []Model.Cluster) (sucesso bool) {
	hashesParaDeletar := GetHash(clusters)
	errou := EscreverTexto(hashesParaDeletar, "hashParaDeletar.txt")
	if errou != nil {
		fmt.Println("Erro em escrever os hash para deletar")
		return false
	}

	return true
}
