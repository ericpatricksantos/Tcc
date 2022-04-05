package Controller

import (
	"fmt"
	"Tcc/Shared/API"
	"Tcc/Shared/Function"
	"Tcc/Shared/Model"
	"strconv"
)

// SaveTxs Salva todas as transações de um Block no MongoDB
func SaveTxs(Txs []int, urlAPI string, rawTx string, ConnectionMongoDB string, DataBaseMongo string, Collection string, FileLogHash string, indiceInicial int) (TransacaoSalva bool, FinalizaExecucao bool) {
	//indiceInicial := Function.GetIndiceLogIndice(FileLogHash) + 1
	for contador := indiceInicial; contador < len(Txs); contador++ {
		confirm, finalizaExecucao := SaveTx(strconv.Itoa(Txs[contador]), urlAPI, rawTx, ConnectionMongoDB, DataBaseMongo, Collection)
		if !confirm {
			fmt.Println("Não foi salvo a transação ", Txs[contador])
			if !finalizaExecucao {
				fmt.Println("Não é necessario finalizar a execução")
				fmt.Println("Indice Atualizado para ", contador)
				temp := []string{strconv.Itoa(contador)}
				Function.EscreverTexto(temp, FileLogHash)
				return confirm, finalizaExecucao
			}
			return confirm, finalizaExecucao
		} else {
			fmt.Println("Salvo a Transação: Nº ", Txs[contador])
			fmt.Println("Indice Atualizado para ", contador)
			temp := []string{strconv.Itoa(contador)}
			Function.EscreverTexto(temp, FileLogHash)
			return confirm, finalizaExecucao
		}
		//fmt.Println("Salvo a Transação: Nº ", Txs[contador])
		//temp := []string{strconv.Itoa(contador)}
		//Function.EscreverTexto(temp, FileLogHash)
		//fmt.Println("Indice Atualizado para ", contador)
		//
		//time.Sleep(time.Minute * time.Duration(tempo))
	}
	fmt.Println("Indice Atual: ", indiceInicial, "Não foi salvo")
	return false, true
}

// SaveTx Salva as Transações no MongoDb consultando pelo API Blockchain
func SaveTx(hash string, urlAPI string, rawTx string, ConnectionMongoDB string, DataBaseMongo string, Collection string) (TransacaoSalva bool, FinalizaExecucao bool) {
	txIndex, _ := strconv.Atoi(hash)
	check := Function.CheckTxIndex(ConnectionMongoDB, DataBaseMongo, Collection, "tx_index", txIndex)

	if check {
		fmt.Println("TxIndex: ", txIndex)
		fmt.Println("Essa Transação existe na Collection ", Collection)
		return false, false
	}
	tx := GetTx(hash, urlAPI, rawTx)

	if len(tx.Inputs) < 1 && len(tx.Hash) > 0 {

		fmt.Println()
		fmt.Println("O objeto Tx retornado não foi salvo, porque a lista de inputs está vázia")
		fmt.Println("O hash dessa transação foi salvo em TxAddrEmpty para ser verificado")
		Function.EscreverTextoSemApagar([]string{tx.Hash}, "..\\Tcc\\TxAddrEmpty.txt")
		fmt.Println()
		return false, false
	}

	if len(tx.Hash) > 0 {
		resposta := Function.SaveTx(tx, ConnectionMongoDB, DataBaseMongo, Collection)
		if resposta {
			return true, false
		} else {
			return false, true
		}
	} else {
		fmt.Println()
		fmt.Println("Erro: Não foi possível realizar a Requisição pela Transação")
		fmt.Println("Erro: O objeto retornado pela API não é uma Transação válida")
		fmt.Println()
		return false, true
	}
}

// GetTx Get Transação da API da Blockchain
func GetTx(hash, urlAPI, rawTx string) Model.Transaction {
	return API.GetTransaction(hash, urlAPI, rawTx)
}

func SalveTxMongoDB(tx Model.Transaction, ConnectionMongoDB, DataBaseMongo, Collection string) bool {
	result, _ := Function.SalveTxMongoDB(tx, ConnectionMongoDB, DataBaseMongo, Collection)
	return result
}

func DeleteTxMongo(hash string, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {
	return Function.DeleteTxMongo(hash, ConnectionMongoDB, DataBaseMongo, Collection)
}

func GetTxMongoDB(ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) Model.Transaction {
	return Function.GetTxMongoDB(ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)
}
