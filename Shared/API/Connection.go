package API

import (
	"Tcc/Shared/Model"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

func GetLatestBlock(urlAPI string, lastestBlock string) Model.LatestBlock {

	fmt.Println("Calling API...")
	client := &http.Client{}
	req, err := http.NewRequest("GET", urlAPI+lastestBlock, nil)
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função NewRequest que esta sendo chamada na Função GetLatestBlock - {API/Connection.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		panic(err)
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)

	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função Do que esta sendo chamada na Função GetLatestBlock - {API/Connection.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		panic(err)
	}

	defer resp.Body.Close()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função ReadAll que esta sendo chamada na Função GetLatestBlock - {API/Connection.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		panic(err)
	}
	var responseObject Model.LatestBlock
	json.Unmarshal(bodyBytes, &responseObject)

	return responseObject
}

func GetMultiEnderecos(enderecos []string, urlAPI string, multiAddr string) Model.MultiEndereco {

	fmt.Println("Calling API...")
	client := &http.Client{}
	req, err := http.NewRequest("GET", urlAPI+multiAddr+strings.Join(enderecos, "|"), nil)
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função NewRequest que esta sendo chamada na Função GetMultiEnderecos - {API/Connection.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		panic(err)
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)

	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função Do que esta sendo chamada na Função GetMultiEnderecos - {API/Connection.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		panic(err)
	}

	defer resp.Body.Close()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função ReadAll que esta sendo chamada na Função GetMultiEnderecos - {API/Connection.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		panic(err)
	}
	var responseObject Model.MultiEndereco
	json.Unmarshal(bodyBytes, &responseObject)

	return responseObject
}

func GetMultiAddr(enderecos []string, urlAPI string, multiAddr string, limit ,offset int) Model.MultiEndereco {

	fmt.Println("Calling API...")
	client := &http.Client{}
	req, err := http.NewRequest("GET", urlAPI+multiAddr+strings.Join(enderecos, "|")+"&limit="+strconv.Itoa(limit)+"&offset="+strconv.Itoa(offset), nil)
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função NewRequest que esta sendo chamada na Função GetMultiAddr - {API/Connection.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		return Model.MultiEndereco{}
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)

	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função Do que esta sendo chamada na Função GetMultiAddr - {API/Connection.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		return Model.MultiEndereco{}
	}

	defer resp.Body.Close()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função ReadAll que esta sendo chamada na Função GetMultiAddr - {API/Connection.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		return Model.MultiEndereco{}
	}
	var responseObject Model.MultiEndereco
	json.Unmarshal(bodyBytes, &responseObject)

	return responseObject
}

func GetUnicoEndereco(endereco string, urlAPI string, RawAddr string) Model.UnicoEndereco {
	fmt.Println("Calling API...")
	client := &http.Client{}
	req, err := http.NewRequest("GET", urlAPI+RawAddr+endereco+"?limit=10000000000",nil)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função NewRequest que esta sendo chamada na Função GetUnicoEndereco - {API/Connection.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		panic(err)
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)

	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Do que esta sendo chamada na Função GetUnicoEndereco - {API/Connection.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		panic(err)
	}

	defer resp.Body.Close()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função ReadAll que esta sendo chamada na Função GetUnicoEndereco - {API/Connection.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		panic(err)
	}
	var responseObject Model.UnicoEndereco
	json.Unmarshal(bodyBytes, &responseObject)

	return responseObject
}

func GetEndereco(endereco string, urlAPI string, RawAddr string, limit ,offset int) Model.Endereco {
	fmt.Println("Calling API...")
	client := &http.Client{}
	req, err := http.NewRequest("GET", urlAPI+RawAddr+endereco+"?limit="+strconv.Itoa(limit)+"&offset="+strconv.Itoa(offset), nil)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função NewRequest que esta sendo chamada na Função GetEndereco - {API/Connection.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		return Model.Endereco{}
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)

	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função Do que esta sendo chamada na Função GetEndereco - {API/Connection.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		return Model.Endereco{}
	}

	defer resp.Body.Close()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função ReadAll que esta sendo chamada na Função GetEndereco - {API/Connection.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		return Model.Endereco{}
	}
	var responseObject Model.Endereco
	json.Unmarshal(bodyBytes, &responseObject)

	return responseObject
}

func GetTransaction(hashTransacao string, urlAPI string, rawTx string) Model.Transaction {
	fmt.Println("Calling API...")
	client := &http.Client{}
	req, err := http.NewRequest("GET", urlAPI+rawTx+hashTransacao, nil)
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função NewRequest que esta sendo chamada na Função GetTransaction - {API/Connection.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		return Model.Transaction{}
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)

	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função Do que esta sendo chamada na Função GetTransaction - {API/Connection.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		panic(err)
	}

	defer resp.Body.Close()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função ReadAll que esta sendo chamada na Função GetTransaction - {API/Connection.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		panic(err)
	}
	var responseObject Model.Transaction
	json.Unmarshal(bodyBytes, &responseObject)

	return responseObject
}

func GetBloco(hashBlock string, urlAPI string, rawBlock string) Model.Block {

	fmt.Println("Calling API...")
	client := &http.Client{}
	req, err := http.NewRequest("GET", urlAPI+rawBlock+hashBlock, nil)
	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função NewRequest que esta sendo chamada na Função GetBloco - {API/Connection.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		panic(err)
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")

	resp, err := client.Do(req)

	if err != nil {

		fmt.Println()
		fmt.Println("Erro na resposta da função Do que esta sendo chamada na Função GetBloco - {API/Connection.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		panic(err)
	}

	defer resp.Body.Close()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println()
		fmt.Println("Erro na resposta da função ReadAll que esta sendo chamada na Função GetBloco - {API/Connection.go}")
		fmt.Println()

		fmt.Println()
		fmt.Println(err.Error())
		fmt.Println()

		panic(err)
	}
	var responseObject Model.Block
	json.Unmarshal(bodyBytes, &responseObject)

	return responseObject
}
