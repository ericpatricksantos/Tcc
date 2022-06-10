package main

import (
	"Tcc/Shared/Function"
	"Tcc/Shared/Model"
	"fmt"
)

var limit int64 = 100
var ConnectionMongo string = "mongodb://127.0.0.1:27017/"
var DB_Endereco string = "Endereco"
var DB_Cluster string = "Cluster"
var Collection_Cluster_Map string = "Clusters"

func main() {
	VerificaEnderecosTotaisClusters()

	ContagemIdentificadores()
}

func TransferindoProd() {
	f := Function.TransfereClustersD1_D2(ConnectionMongo, "Cluster", "Cluster",
		"Identificadores_1", "Distancia1", "Identificadores_2", "Distancia2",
		"Cluster", "Identificadores", "Clusters")

	fmt.Println(f)
}

func TransferindoDev() {
	f := Function.TransfereClustersD1_D2(ConnectionMongo, "Cluster", "Cluster",
		"Identificadores_1", "Distancia1", "Identificadores_2", "Distancia2",
		"teste", "Identificadores", "Clusters")

	fmt.Println(f)
}

func ContagemEnderecosTotais() {
	r := Function.ContagemEnderecosTotais(ConnectionMongo, DB_Cluster, "Distancia1", "Distancia2")

	// Total 1530531 endereços distancia 1 e 2
	fmt.Println(len(r))
}

func VerificaEnderecosTotaisClusters() {
	f := Function.ContagemEnderecosClusters(ConnectionMongo, "Cluster", "Clusters")
	// Total 1530531 endereços
	fmt.Println(len(f))
}

func VerificaQtdTransacoesTotais() {
	v := Function.UnionHashTransacaoD1_D2(ConnectionMongo, DB_Endereco, "Farao", "Distancia1")
	// Distancia 1 - 278 transações/clusters
	// Distancia 2 - 633314 transações/clusters

	// Total 633592 transações/clusters
	fmt.Println(len(v))
}

func VerificaEnderecosTotais() {
	v := Function.UnionEnderecosD1_D2(ConnectionMongo, DB_Endereco, "Farao", "Distancia1")
	// Distancia 1 -  6528 endereços
	// Distancia 2 - 1524003 endereços

	// Total 1530531 endereços
	fmt.Println(len(v))
}

func VerificaQtdEnderecosDistancia1() {
	colDist1 := "Distancia1"
	clusters := Function.GetAllClustersLimit(100000, ConnectionMongo, DB_Cluster, colDist1)
	lista := []string{}

	for _, cluster := range clusters {
		for _, endereco := range cluster.Clusters {
			check := Function.Contains(lista, endereco)
			if check {
				continue
			} else {
				lista = append(lista, endereco)
			}
		}
	}
	if len(lista) == 6528 {
		fmt.Println(" Correto")
	}

}

// em uso
func VerificaCreateClustersDistancia1_Map() {
	colDist1 := "Distancia1"
	clusters := Function.GetAllMapClustersLimit(100000, ConnectionMongo, DB_Cluster, colDist1)
	mapas := map[string]string{}

	for _, cluster := range clusters {
		for endereco, _ := range cluster.Clusters {
			_, ok := mapas[endereco]
			if ok {
				continue
			} else {
				mapas[endereco] = endereco
			}
		}
	}

	if len(mapas) == 6528 {
		fmt.Println(" Correto")
	}
}

func TesteDistancia() {
	ConnectionMongoDB := "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
	Database := "Endereco"
	Collection := "Distancia1"

	valores := Function.GetAllAddrLimit(100000000, ConnectionMongoDB, Database, Collection)
	dic := map[string]string{}

	for _, item := range valores {
		_, ok := dic[item.Address]
		if ok {

		} else {
			dic[item.Address] = item.Address
		}
	}

	fmt.Println(len(dic))
}

func TesteDistancia1Informacoes() {
	inputs := []string{}
	txs := []string{}
	enderecos := Function.GetAllAddr("mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb",
		"Endereco", "Farao")

	for _, endereco := range enderecos {
		for _, tx := range endereco.Txs {
			txs = append(txs, tx.Hash)
			for _, input := range tx.Inputs {
				inputs = append(inputs, input.Prev_Out.Addr)
			}
		}
	}

	inputUnicos, _ := Function.RemoveDuplicados(inputs)
	txsUnicos, _ := Function.RemoveDuplicados(txs)

	info := Model.Informacoes{
		InfoEnderecos: Model.InfoEnderecos{
			Enderecos:    inputUnicos,
			QtdEnderecos: len(inputUnicos),
		},
		InfoTransacoes: Model.InfoTransacoes{
			Transacoes:    txsUnicos,
			QtdTransacoes: len(txsUnicos),
		},
	}

	Function.SaveInfo(info, "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb",
		"Endereco", "Distancia1Informacoes")
}

func TesteEndereco_indice_2053() {
	ConnectionMongoDB := "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
	Database := "Endereco"
	Collection := "Endereco_2053"
	enderecos := Function.GetAllAddrLimit(1000000, ConnectionMongoDB,
		Database, Collection)
	dic := map[string]string{}
	//hashes := []string{}
	for _, addr := range enderecos {
		for _, tx := range addr.Txs {
			//hashes = append(hashes, tx.Hash)
			_, ok := dic[tx.Hash]
			if ok {
				continue
			} else {
				dic[tx.Hash] = tx.Hash
			}
		}
	}
	//x,_ := Function.RemoveDuplicados(hashes)

	//mt.Println(len(hashes))
	//fmt.Println(len(x))
	fmt.Println(len(dic))
}

func QuantidadeTransacoesDistancia1() {
	ConnectionMongoDB := "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
	Database := "teste"
	Collection := "transacoes"
	enderecos := Function.GetAllAddrLimit(1000000, ConnectionMongoDB,
		Database, Collection)
	dic := map[string]string{}
	hashes := []string{}
	contador := 0
	for _, addr := range enderecos {
		x := len(addr.Txs)
		contador = contador + x
		for _, tx := range addr.Txs {
			hashes = append(hashes, tx.Hash)
			_, ok := dic[tx.Hash]
			if ok {
				continue
			} else {
				dic[tx.Hash] = tx.Hash
			}
		}
	}
	fmt.Println("Qtd: ", contador)
	fmt.Println(len(hashes))
	fmt.Println(len(dic))
}

func ContEnderecosCluster() {
	clusters := Function.GetAllMapClustersLimit(limit, ConnectionMongo, DB_Cluster, Collection_Cluster_Map)
	v := map[string]string{}

	for _, item := range clusters {
		for ch, _ := range item.Clusters {
			_, ok := v[ch]
			if !ok {
				v[ch] = ""
			}
		}
	}

	fmt.Println(len(v))
}

func ContagemIdentificadores() {
	f := Function.ContagemIdentificadoresClusters(ConnectionMongo, "Cluster", "Clusters")
	fmt.Println(len(f))
}
