package Service

import (
	"fmt"
	"Tcc/Shared/API"
	"Tcc/Shared/Config"
	Function2 "Tcc/Shared/Function"
	"Tcc/Shared/Model"
	"strconv"
)

/*
	Implementar o algoritmo H1
*/

func H1(ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string, IgnoraCluster int, removeClusterValorDefinido bool, valorDefinido int) (confirmH1, erro, executeAll, pausaExecucao bool) {
	valoresHashParaDeletar, _ := Function2.LerTexto("hashParaDeletar.txt")

	if len(valoresHashParaDeletar) > 0 {
		conf := Function2.DeletarListHash(valoresHashParaDeletar, "hashParaDeletar.txt",
			ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)
		if conf {
			return false, false, false, false
		} else {
			return false, true, false, true
		}
	} else {
		var limitClusters int64 = 20000
		var offset int64 = 19952
		fmt.Println("Buscando todos os clusters")
		clusters := Function2.GetAllClusterLimitOffset(limitClusters, offset, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)
		if len(clusters) < 1 {
			fmt.Println()
			fmt.Println("Não tem nenhum cluster")
			return false, false, false, true
		}
		indiceInicialCluster, indiceInicialInput :=
			Function2.BuscaDoisIndices("IndiceCluster.txt",
				"IndiceInputCluster.txt")

		tamanhoCluster := len(clusters)

		if (tamanhoCluster < indiceInicialCluster) && (int(limitClusters) == tamanhoCluster) {
			indiceInicialCluster = 0
		} else if int(limitClusters) != tamanhoCluster {
			return false, false, false, false
		}

		for index_cluster := indiceInicialCluster; index_cluster < tamanhoCluster; index_cluster++ {

			identificarCluster := clusters[index_cluster].Hash
			tamanhoInput := len(clusters[index_cluster].Input)
			listaInput := clusters[index_cluster].Input
			fmt.Println("Identificação o Cluster(Hash): ", identificarCluster)
			fmt.Println("Tamanho da lista de addr(Input || cluster): ", tamanhoInput)
			fmt.Println()
			if tamanhoInput < indiceInicialInput {
				indiceInicialInput = 0
				Function2.DefineIndice(0, "IndiceInputCluster.txt")
			}

			if tamanhoInput < IgnoraCluster {
				for index_input := indiceInicialInput; index_input < tamanhoInput; index_input++ {

					endereco := clusters[index_cluster].Input[index_input]

					fmt.Println("Quantidade de Cluster Restantes: ", tamanhoCluster)
					fmt.Println("Indice do Cluster: ", index_cluster)
					fmt.Println("Indice do input: ", index_input)
					fmt.Println()

					fmt.Println("Buscando o endereço ", endereco, " na lista de inputs de outro cluster")
					resultSearch := Function2.SearchClustersLimit(28, endereco, ConnectionMongoDB,
						DataBaseMongo, CollectionRecuperaDados)

					quantidadeCusterEncontrados := len(resultSearch)
					fmt.Println()
					if quantidadeCusterEncontrados == 2 {
						fmt.Println(" Posiçao 0 Hash: ", resultSearch[0].Hash,
							" Tamanho Input(cluster): ", len(resultSearch[0].Input))
						fmt.Println(" Posiçao 1 Hash: ", resultSearch[1].Hash,
							" Tamanho Input(cluster): ", len(resultSearch[1].Input))
						fmt.Println()
					} else if quantidadeCusterEncontrados == 1 {
						fmt.Println(" Posiçao 0 Hash: ", resultSearch[0].Hash,
							" Tamanho Input(cluster): ", len(resultSearch[0].Input))
						fmt.Println()
					} else if quantidadeCusterEncontrados == 0 {
						fmt.Println("Erro: O valor minimo retornado é sempre 1")
						return false, true, false, true
					}
					if quantidadeCusterEncontrados > 1 {
						fmt.Println("Quantidade de Clusters Encontrados: ", quantidadeCusterEncontrados)
						fmt.Println("Encontrado o endereço ", endereco, " em uma lista de inputs de um cluster")

						clustersComValorDefinido :=
							Function2.RemoveClusterComValorDefinido(removeClusterValorDefinido,
								valorDefinido, resultSearch)
						if clustersComValorDefinido == nil && removeClusterValorDefinido {
							fmt.Println("Clusters acima do valor definido")
							Function2.DefineIndice(0, "IndiceInputCluster.txt")
							fmt.Println("Indice do input atualizado ", 0)
							Function2.IncrementaIndice(index_cluster, "IndiceCluster.txt")
							fmt.Println("Indice do Cluster atualizado ", index_cluster+1)
							return false, false, false, false
						} else if len(clustersComValorDefinido) == 0 && removeClusterValorDefinido {
							fmt.Println("Clusters acima do valor definido")
							Function2.DefineIndice(0, "IndiceInputCluster.txt")
							fmt.Println("Indice do input atualizado ", 0)
							Function2.IncrementaIndice(index_cluster, "IndiceCluster.txt")
							fmt.Println("Indice do Cluster atualizado ", index_cluster+1)
							return false, false, false, false
						}

						fmt.Println("Removendo o cluster : ", identificarCluster, " da lista de clusters retornados")
						clustersSemOClusterAtual :=
							Function2.RemoveCluster(identificarCluster, clustersComValorDefinido)
						fmt.Println("Removido com sucesso o cluster: ", identificarCluster, " da lista de clusters retornados")

						if clustersSemOClusterAtual == nil {
							fmt.Println("clustersSemOClusterAtual esta vazio")
							Function2.DefineIndice(0, "IndiceInputCluster.txt")
							fmt.Println("Indice do input atualizado ", 0)
							Function2.IncrementaIndice(index_cluster, "IndiceCluster.txt")
							fmt.Println("Indice do Cluster atualizado ", index_cluster+1)
							return false, false, false, false
						} else if len(clustersSemOClusterAtual) == 0 {
							fmt.Println("clustersSemOClusterAtual esta vazio")
							Function2.DefineIndice(0, "IndiceInputCluster.txt")
							fmt.Println("Indice do input atualizado ", 0)
							Function2.IncrementaIndice(index_cluster, "IndiceCluster.txt")
							fmt.Println("Indice do Cluster atualizado ", index_cluster+1)
							return false, false, false, false
						}

						confirmacaoSalvaLista := Function2.SalvarListaCluster(clustersSemOClusterAtual)
						if !confirmacaoSalvaLista {
							fmt.Println("Erro em escrever os hash para deletar")
							return false, true, false, true
						}
						fmt.Println("Removendo os itens duplicados da propria lista de inputs(clusters)" +
							" que foi buscada")
						listaEnderecosUnicos, tamlistaEnderecosUnicos :=
							Function2.RemoveItem(
								UnionCluster(clustersSemOClusterAtual), endereco)
						fmt.Println("Removido com sucesso os itens duplicados da propria lista de inputs(clusters) " +
							"que foi buscada")

						if tamlistaEnderecosUnicos == 0 || listaEnderecosUnicos == nil {
							fmt.Println("A listaEnderecosUnicos esta vazia")
							hashesParaDeletar := Function2.GetHash(clustersSemOClusterAtual)
							fmt.Println("Quantidade de clusters que serão deletados: ", len(hashesParaDeletar))
							DeleteConfirm := DeleteListCluster(hashesParaDeletar, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)

							if !DeleteConfirm {
								fmt.Println()
								fmt.Println("Não foram deletados todos os clusters")
								return false, false, false, true
							} else {
								fmt.Println()
								fmt.Println("Deletado com sucesso")

								Function2.DefineIndice(0, "IndiceInputCluster.txt")
								fmt.Println("Indice do input atualizado ", 0)
								Function2.IncrementaIndice(index_cluster, "IndiceCluster.txt")
								fmt.Println("Indice do Cluster atualizado ", index_cluster+1)

								Function2.LimpaTxt("hashParaDeletar.txt")
								fmt.Println("limpa o hashParaDeletar")

								return false, false, false, false
							}
						}

						fmt.Println("Removendo os duplicados de listaEnderecosUnicos "+
							"em comparação com o Input do Cluster Atual: ", identificarCluster)
						clusterResultante, tamClusterResultante := Function2.EliminaElem(listaEnderecosUnicos, listaInput)
						fmt.Println("Remoção concluida dos duplicados")
						fmt.Println()

						fmt.Println("Tamanho do cluster que sera adicionado: ", tamClusterResultante)

						if (tamanhoInput + tamClusterResultante) > 300000 {
							fmt.Println()
							Function2.DefineIndice(0, "IndiceInputCluster.txt")
							Function2.IncrementaIndice(index_cluster, "IndiceCluster.txt")
							Function2.LimpaTxt("hashParaDeletar.txt")
							fmt.Println("Valor desse cluster: ", tamanhoInput+tamClusterResultante)
							fmt.Println("O cluster que será salvo passara de 300000")
							fmt.Println()
							return false, false, false, true
						}
						confirmProcessamento, erroProcessamento, ExecuteAll, PausaExecucao := ProcessamentoCluster(clusterResultante,
							index_cluster, index_input, tamClusterResultante,
							identificarCluster, ConnectionMongoDB,
							DataBaseMongo, CollectionRecuperaDados)

						return confirmProcessamento, erroProcessamento, ExecuteAll, PausaExecucao

					} else {
						fmt.Println("O endereço ", endereco, " não foi encontrado na lista de inputs de outro cluster")
						Function2.IncrementaIndice(index_input, "IndiceInputCluster.txt")
						fmt.Println("Indice do input atualizado ", index_input+1)
						fmt.Println()
						return false, false, false, false
					}

				} // Fim do For que percorre a lista de Inputs(clusters)

				Function2.DefineIndice(0, "IndiceInputCluster.txt")
				fmt.Println("Indice do input atualizado para 0")
				indiceInicialInput = 0

				Function2.IncrementaIndice(index_cluster, "IndiceCluster.txt")
				fmt.Println("Indice do cluster atualizado para ", index_cluster+1)

			} else {
				fmt.Println("O tamanho da lista endereco é maior que : ", IgnoraCluster,
					", por isso o Cluster : ", identificarCluster, " foi ignorado")
				fmt.Println("Tamanho desse cluster: ", tamanhoInput)

				Function2.DefineIndice(0, "IndiceInputCluster.txt")
				fmt.Println("Indice do input atualizado ", 0)

				Function2.IncrementaIndice(index_cluster, "IndiceCluster.txt")
				fmt.Println("Indice do Cluster atualizado ", index_cluster+1)
				indiceInicialInput = 0
			}

		} // Fim do For que percorre os clusters

		Function2.DefineIndice(0, "IndiceCluster.txt")
		fmt.Println("Indice do cluster atualizado para ", 0)
		Function2.DefineIndice(0, "IndiceInputCluster.txt")
		fmt.Println("Indice do input atualizado para 0")

		return true, false, true, true
	}

}

func ProcessamentoCluster(clusterResultante []string, index_cluster, index_input, tamClusterResultante int, identificarCluster, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados string) (confirmH1, erro, executeAll, pausaExecucao bool) {
	var SaveConfirm bool
	var erroAddAll bool
	if tamClusterResultante > 0 && tamClusterResultante < 301 {
		fmt.Println("Adicionando a lista no Cluster: ", identificarCluster)
		SaveConfirm, erroAddAll, _ = Function2.AddListToList(identificarCluster,
			clusterResultante, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)
	} else if tamClusterResultante >= 301 {
		fmt.Println("Adicionando a lista no Cluster: ", identificarCluster)
		SaveConfirm, erroAddAll = Function2.AddAll(identificarCluster,
			clusterResultante, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)
	}

	if SaveConfirm {
		fmt.Println("Adicionado com sucesso a lista no Cluster: ", identificarCluster)
		hashesParaDeletar, _ := Function2.LerTexto("hashParaDeletar.txt")
		DeleteConfirm := DeleteListCluster(hashesParaDeletar,
			ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)

		if !DeleteConfirm {
			fmt.Println()
			fmt.Println("Foi adicionado a lista ao Cluster: ",
				identificarCluster, ", mas nao foi apagado os clusters adicionados")
			fmt.Println("Não foram deletados todos os clusters")
			return false, true, false, true
		}

		Function2.DefineIndice(0, "IndiceInputCluster.txt")
		fmt.Println("Indice do input atualizado ", 0)

		Function2.IncrementaIndice(index_cluster, "IndiceCluster.txt")
		fmt.Println("Indice do Cluster atualizado ", index_cluster+1)

		fmt.Println(" Quantidade de clusters(objetos) apagados: ", len(hashesParaDeletar))

		Function2.LimpaTxt("hashParaDeletar.txt")
		fmt.Println("O hashParaDeletar.txt foi apagado")

		fmt.Println("Lista de Endereços Atualizado")

		return true, false, false, false
	} else if !SaveConfirm && erroAddAll {
		fmt.Println("Cluster Resultante não foi Atualizado")
		fmt.Println("Limpa o hashParaDeletar.txt para os valores do" +
			" hash nao serem apagados na proxima execução")
		Function2.LimpaTxt("hashParaDeletar.txt")

		return false, true, false, true
	} else {
		if tamClusterResultante > 0 {
			fmt.Println()
			fmt.Println("Erro: Cluster Resultante não foi Atualizado")
			Function2.IncrementaIndice(index_input, "IndiceInputCluster.txt")
			fmt.Println("Indice do input atualizado ", index_input+1)
			return false, true, false, true
		} else {
			hashesParaDeletar, _ := Function2.LerTexto("hashParaDeletar.txt")
			if hashesParaDeletar == nil {
				fmt.Println()
				fmt.Println("Não existe nenhum valor em hashParaDeletar.txt")
				return false, true, false, true
			} else if len(hashesParaDeletar) == 0 {
				fmt.Println()
				fmt.Println("Não existe nenhum valor em hashParaDeletar.txt")
				return false, true, false, true
			}
			DeleteConfirm := DeleteListCluster(hashesParaDeletar, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)

			if !DeleteConfirm {
				fmt.Println()
				fmt.Println("Não foram deletados todos os clusters")
				return false, false, false, true
			} else {
				fmt.Println()
				fmt.Println("Todos os valores do cluster resultante ja tem no cluster atual, " +
					"por isso apaga os clusters que foram buscados")
				fmt.Println("Deletado com sucesso os clusters que foram buscados")
				Function2.IncrementaIndice(index_input, "IndiceInputCluster.txt")
				Function2.LimpaTxt("hashParaDeletar.txt")
				fmt.Println("limpa o hashParaDeletar")
				return false, false, false, false
			}
		}

	}
	return false, true, false, true
}

func UnionCluster(clusters []Model.Cluster) (result []string) {
	for _, item := range clusters {
		result = append(result, item.Input...)
	}

	result, _ = Function2.RemoveDuplicados(result)

	return result
}

func SearchAddr(addr string, ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) []Model.Cluster {
	return Function2.SearchClusters(addr, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)
}

func DeleteListCluster(clusters []string, ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) bool {
	if clusters == nil {
		fmt.Println()
		fmt.Println(" O tamanho da lista de clusters está vazia")
		return false
	} else if len(clusters) == 0 {
		fmt.Println()
		fmt.Println(" O tamanho da lista de clusters está vazia")
		return false
	} else {
		for _, item := range clusters {
			fmt.Println("Deletando o cluster: ", item)

			confirm := Function2.DeleteCluster(item, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)

			if !confirm {
				fmt.Println("Não foi deletado o cluster: ", item)
				return false
			}
			fmt.Println("Deletado com sucesso o cluster: ", item)
		}
		fmt.Println("Todos os clusters foram deletados")
		return true
	}
}

func CreateCluster(ConnectionMongoDB, DataBaseTx, CollectionTx, DataBaseCluster,
	CollectionCluster string) (confirmCluster bool, FinalizaExecucao bool) {

	Txs := Function2.GetAllTxs(ConnectionMongoDB, DataBaseTx, CollectionTx)
	QtdEnderecosVazios := 0
	Tentativas := 2

	Hash := Txs[0].Hash

	if len(Txs) < 1 {
		fmt.Println("******** Não têm Transações para serem processadas **")
		return false, true
	} else if len(Txs[0].Inputs) < 1 {
		fmt.Println("******** A lista de Inputs está vazio **")
		fmt.Println("******** A Transação será salva em processed **")
		processed := Config.GetConfig().Collection[2]
		mudou := Function2.MudancaStatusTx(Txs[0], ConnectionMongoDB, DataBaseTx, CollectionTx, processed)

		if mudou {
			fmt.Println("Mudança de status concluida ", CollectionTx, " >> ", processed)
		} else {
			fmt.Println("Falha na mudança de status ", CollectionTx, " >> ", processed)
			return false, true
		}

		return false, false
	} else {
		for k := 0; k < Tentativas; k++ {
			if QtdEnderecosVazios > 0 {
				fmt.Println("******** Tentando recuperar os endereços que estão na lista de inputs **")

				urlAPI := Config.GetConfig().UrlAPI[0]
				rawTx := Config.GetConfig().RawTx

				txTemp := API.GetTransaction(Hash, urlAPI, rawTx)
				Txs = []Model.Transaction{txTemp}

				QtdEnderecosVazios = 0
			}

			for i := 0; i < len(Txs); i++ {
				var Cluster Model.Cluster
				var inputs []string
				Cluster.Hash = Txs[i].Hash
				for j := 0; j < len(Txs[i].Inputs); j++ {
					if len(Txs[i].Inputs[j].Prev_Out.Addr) > 0 {
						inputs = append(inputs, Txs[i].Inputs[j].Prev_Out.Addr)
					} else {
						QtdEnderecosVazios++
					}
				}
				lenInputs := len(Txs[i].Inputs)
				if QtdEnderecosVazios == lenInputs && k == 0 {
					fmt.Println("******** Na Hash: ", Txs[i].Hash)
					fmt.Println("******** Os enderecos da lista de inputs estão vazios **")
					fmt.Println("******** Tamanho da lista de Inputs", lenInputs, " **")
					break
				} else if QtdEnderecosVazios > 0 && QtdEnderecosVazios < lenInputs && k == 0 {
					fmt.Println("******** Na Hash: ", Txs[i].Hash, " **")
					fmt.Println("******** Existem ", QtdEnderecosVazios, " vazios dentro da lista de inputs **")
					fmt.Println("******** Tamanho da lista de Inputs", lenInputs, " **")
					break
				} else if QtdEnderecosVazios == lenInputs && k == 1 {
					fmt.Println("******** Tentantiva de recuperar os endereços do input foi falha, pois todos permaneceram vazios **")

					fmt.Println("******** Salvando a Transação em processed **")
					processed := Config.GetConfig().Collection[2]

					mudou := Function2.MudancaStatusTx(Txs[i], ConnectionMongoDB, DataBaseTx, CollectionTx, processed)

					if mudou {
						fmt.Println("Mudança de status concluida ", CollectionTx, " >> ", processed)
					} else {
						fmt.Println("Falha na mudança de status ", CollectionTx, " >> ", processed)
						return false, true
					}
					fmt.Println("******** O hash dessa Transação será salvo em um arquivo chamado AddrInputEmpty.txt **")
					Function2.EscreverTextoSemApagar([]string{Hash}, "..\\Tcc\\AddrInputEmpty.txt")
					return false, false

				} else if QtdEnderecosVazios > 0 && QtdEnderecosVazios < lenInputs && k == 1 {
					fmt.Println("******** Na Hash: ", Txs[i].Hash)
					fmt.Println("******** Existem ", QtdEnderecosVazios, " vazios dentro da lista de inputs **")
					fmt.Println("******** Tamanho Inputs: ", lenInputs)
					fmt.Println("******** Será criado o Cluster com os endereços que estão preenchidos na lista de inputs **")
				}

				Cluster.Input, _ = Function2.RemoveDuplicados(inputs)
				confirm, existente := Function2.SaveClusterMongo(Cluster, ConnectionMongoDB, DataBaseCluster, CollectionCluster)

				if confirm {
					return true, false
				} else if !confirm && existente {
					processed := Config.GetConfig().Collection[2]
					mudou := Function2.MudancaStatusTx(Txs[i], ConnectionMongoDB, DataBaseTx, CollectionTx, processed)

					if mudou {
						fmt.Println("Mudança de status concluida ", CollectionTx, " >> ", processed)
					} else {
						fmt.Println("Falha na mudança de status ", CollectionTx, " >> ", processed)
						return false, true
					}
					return false, false
				} else {
					return false, true
				}
			}
		}
	}
	return false, true
}

func CreateClustersAddr(ConnectionMongoDB, DataBaseCluster, CollectionCluster,
	DataBaseAddr, processed, processedCluster,
	processedAddrAnalise, processedAddrAnaliseCluster string, NewAddrAnalise bool) (createClusterSucess bool, FinalizaExecucao bool) {
	var Cluster Model.Cluster
	tamanhoTxAddrOutrosNiveis := 0
	tamanhoAddrOutrosNiveis := 0
	tamanhoTxAddrAnalise := 0
	tamanhoAddrAnalise := 0
	enderecosEmAnalise := Model.UnicoEndereco{}
	enderecosOutrosNiveis := Model.UnicoEndereco{}
	if NewAddrAnalise {
		enderecosEmAnalise = Function2.GetAddrMongoDB(ConnectionMongoDB, DataBaseAddr, processedAddrAnalise)
		tamanhoTxAddrAnalise = len(enderecosEmAnalise.Txs)
		tamanhoAddrAnalise = len(enderecosEmAnalise.Address)
	}

	if tamanhoAddrAnalise > 0 && tamanhoTxAddrAnalise > 0 {
		inputs := Function2.GetAllInputs(enderecosEmAnalise)
		if tamanhoTxAddrAnalise > 0 && tamanhoAddrAnalise > 0 {
			Cluster.Hash = enderecosEmAnalise.Address
			Cluster.Input = inputs
		} else {
			fmt.Println("Não foi criado o cluster do endereço ", enderecosEmAnalise.Address)
			return false, true
		}

	} else {
		enderecosOutrosNiveis = Function2.GetAddrMongoDB(ConnectionMongoDB, DataBaseAddr, processed)
		tamanhoTxAddrOutrosNiveis = len(enderecosOutrosNiveis.Txs)
		tamanhoAddrOutrosNiveis = len(enderecosOutrosNiveis.Address)
		if tamanhoAddrOutrosNiveis > 0 && tamanhoTxAddrOutrosNiveis > 0 {
			inputs := Function2.GetAllInputs(enderecosOutrosNiveis)
			Cluster.Hash = enderecosOutrosNiveis.Address
			Cluster.Input = inputs
		} else {
			fmt.Println("Não foi criado o cluster do endereço ", enderecosOutrosNiveis.Address)
			return false, true
		}
	}
	fmt.Println()
	fmt.Println("Criando clusters com o Address: ", Cluster.Hash)
	fmt.Println()
	if len(Cluster.Hash) > 0 && len(Cluster.Input) > 0 {
		confirm, existente := Function2.SaveClusterMongo(Cluster, ConnectionMongoDB, DataBaseCluster, CollectionCluster)

		if confirm {
			if tamanhoAddrAnalise > 0 && tamanhoTxAddrAnalise > 0 {
				mudou, _ := Function2.MudancaStatusAddr(enderecosEmAnalise, ConnectionMongoDB, DataBaseAddr, processedAddrAnalise, processedAddrAnaliseCluster)
				if mudou {
					fmt.Println("Mudado o status de ", processedAddrAnalise, " >> ", processedAddrAnaliseCluster)
					return true, false
				} else {
					fmt.Println("Não foi mudado o status de ", processedAddrAnalise, " >> ", processedAddrAnaliseCluster)
					return true, true
				}
			} else if tamanhoTxAddrOutrosNiveis > 0 && tamanhoAddrOutrosNiveis > 0 {
				mudou, _ := Function2.MudancaStatusAddr(enderecosOutrosNiveis, ConnectionMongoDB, DataBaseAddr, processed, processedCluster)
				if mudou {
					fmt.Println("Mudado o status de ", processed, " >> ", processedCluster)
					return true, false
				} else {
					fmt.Println("Não foi mudado o status de ", processed, " >> ", processedCluster)
					return true, true
				}
			}
			fmt.Println("Cluster salvo, mas nao mudou o status")
			fmt.Println("Address: ", Cluster.Hash, " para ser analisado")
			return true, true
		} else if !confirm && existente {
			if tamanhoAddrAnalise > 0 && tamanhoTxAddrAnalise > 0 {
				mudou, _ := Function2.MudancaStatusAddr(enderecosEmAnalise, ConnectionMongoDB, DataBaseAddr, processedAddrAnalise, processedAddrAnaliseCluster)
				if mudou {
					fmt.Println("Mudado o status de ", processedAddrAnalise, " >> ", processedAddrAnaliseCluster)
					return false, false
				} else {
					fmt.Println("Não foi mudado o status de ", processedAddrAnalise, " >> ", processedAddrAnaliseCluster)
					return false, true
				}
			} else if tamanhoTxAddrOutrosNiveis > 0 && tamanhoAddrOutrosNiveis > 0 {
				mudou, _ := Function2.MudancaStatusAddr(enderecosOutrosNiveis, ConnectionMongoDB, DataBaseAddr, processed, processedCluster)
				if mudou {
					fmt.Println("Mudado o status de ", processed, " >> ", processedCluster)
					return true, false
				} else {
					fmt.Println("Não foi mudado o status de ", processed, " >> ", processedCluster)
					return false, true
				}
			}
			return false, true
		} else {
			return false, true
		}
	} else {
		fmt.Println("Valores do Hash e inputs do Cluster vazios")
		return false, true
	}
}

func Teste_H1(ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string, IgnoraCluster int, NoCheckNextAddr bool) (confirmH1, erro, executeAll bool) {
	valoresHashParaDeletar, _ := Function2.LerTexto("hashParaDeletar.txt")

	if len(valoresHashParaDeletar) > 0 {
		DeleteConfirm := DeleteListCluster(valoresHashParaDeletar, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)

		if !DeleteConfirm {
			fmt.Println("Não foram deletados todos os clusters")
			return false, false, false
		} else {
			fmt.Println("Deletado")
			fmt.Println("limpa o hashParaDeletar")
			Function2.EscreverTexto([]string{}, "hashParaDeletar.txt")

		}
	} else {

		fmt.Println("Buscando todos os clusters")
		clusters := Function2.GetAllCluster(ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)
		if len(clusters) < 1 {
			fmt.Println("Não tem nenhum cluster")
			return false, true, false
		}
		indiceInicial := Function2.GetIndiceLogIndice("IndiceCluster.txt")
		indiceInicialInput := Function2.GetIndiceLogIndice("IndiceInputCluster.txt")
		tamanhoCluster := len(clusters)

		if tamanhoCluster < indiceInicial {
			indiceInicial = 0
		}

		for index_cluster := indiceInicial; index_cluster < tamanhoCluster; index_cluster++ {

			HashAtual := clusters[index_cluster].Hash
			tamanhoInput := len(clusters[index_cluster].Input)
			listaInput := clusters[index_cluster].Input

			if tamanhoInput < indiceInicialInput {
				indiceInicialInput = 0
			}

			if tamanhoInput < IgnoraCluster {
				for index_input := indiceInicialInput; index_input < tamanhoInput; index_input++ {

					item2 := clusters[index_cluster].Input[index_input]

					fmt.Println("Quantidade de Cluster Restantes: ", tamanhoCluster)
					fmt.Println("Indice do Cluster: ", index_cluster)
					fmt.Println("Tamanho da lista de input: ", tamanhoInput)
					fmt.Println("Indice do input: ", index_input)
					fmt.Println("Buscando o endereço ", item2, " na lista de inputs de outro cluster")
					resultSearch := SearchAddr(item2, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)
					fmt.Println()
					quantidadeCusterEncontrados := len(resultSearch)
					if quantidadeCusterEncontrados > 1 {
						fmt.Println("Encontrado o endereço ", item2, "em uma lista de inputs de um cluster")
						fmt.Println("Quantidade de Clusters Encontrados: ", quantidadeCusterEncontrados)
						//fmt.Println("Verificando se o endereço(hash)", clusters[index_cluster].Hash ,"ainda existe")
						//existeCluster := Function2.CheckCluster(ConnectionMongoDB,DataBaseMongo, CollectionRecuperaDados, "hash", clusters[index_cluster].Hash)
						existeCluster := true
						if existeCluster {
							fmt.Println("Removendo o cluster: ", HashAtual)
							result := Function2.RemoveCluster(HashAtual, resultSearch)
							fmt.Println("Removido com sucesso o cluster: ", HashAtual)
							hashesParaDeletar := Function2.GetHash(result)
							errou := Function2.EscreverTexto(hashesParaDeletar, "hashParaDeletar.txt")
							if errou != nil {
								fmt.Println("Erro em escrever os hash para deletar")
								return false, true, false
							}
							//valores, _ := Function2.RemoveDuplicados(UnionCluster(result))
							//Remove os duplicados e o item pesquisado
							fmt.Println("Removendo os itens duplicados")
							listaResultante, _ := Function2.RemoveItem(UnionCluster(result), item2)
							fmt.Println("Removido com sucesso os itens duplicados")
							fmt.Println("Removendo os duplicados de listaResultante em comparação com o Input")
							clusterResultante, tamClusterResultante := Function2.EliminaElem(listaResultante, listaInput)
							fmt.Println("Remoção concluida")
							fmt.Println("Tamanho do cluster resultante: ", tamClusterResultante)
							fmt.Println("Adicionando a lista no Cluster: ", HashAtual)
							var SaveConfirm bool
							var erroAddAll bool
							if tamClusterResultante < 301 {
								SaveConfirm, erroAddAll, _ = Function2.AddListToList(HashAtual, clusterResultante, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)
							} else {
								SaveConfirm, erroAddAll = Function2.AddAll(HashAtual, clusterResultante, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)
							}

							if SaveConfirm {

								DeleteConfirm := DeleteListCluster(hashesParaDeletar, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)

								if !DeleteConfirm {
									fmt.Println("Não foram deletados todos os clusters")
									return false, false, false
								}
								// limpa o hashParaDeletar
								Function2.EscreverTexto([]string{}, "hashParaDeletar.txt")

								temp := []string{strconv.Itoa(index_input + 1)}
								Function2.EscreverTexto(temp, "IndiceInputCluster.txt")
								fmt.Println()
								fmt.Println("Cluster Resultante Atualizado")
								fmt.Println()

								//if(NoCheckNextAddr){
								//	fmt.Println("Não verifica os próximos endereços da lista de inputs")
								//	fmt.Println("Pula para o próximo cluster")
								//	break
								//}

								return true, false, false
							} else if !SaveConfirm && erroAddAll {
								fmt.Println("Cluster Resultante não foi Atualizado")
								// limpa o hashParaDeletar
								Function2.EscreverTexto([]string{}, "hashParaDeletar.txt")

								return false, true, false
							} else {
								if tamClusterResultante != 0 {
									fmt.Println("Cluster Resultante não foi Atualizado")
									temp1 := []string{strconv.Itoa(index_input + 1)}
									Function2.EscreverTexto(temp1, "IndiceInputCluster.txt")

									fmt.Println("Indice do input atualizado ", index_input+1)
									return false, false, false
								} else {
									DeleteConfirm := DeleteListCluster(valoresHashParaDeletar, ConnectionMongoDB, DataBaseMongo, CollectionRecuperaDados)

									if !DeleteConfirm {
										fmt.Println("Não foram deletados todos os clusters")
										return false, false, false
									} else {
										fmt.Println("Deletado")
										fmt.Println("limpa o hashParaDeletar")
										Function2.EscreverTexto([]string{}, "hashParaDeletar.txt")

									}
								}

							}
						} else {
							temp1 := []string{strconv.Itoa(0)}
							Function2.EscreverTexto(temp1, "IndiceInputCluster.txt")
							fmt.Println()

							temp := []string{strconv.Itoa(index_cluster + 1)}
							Function2.EscreverTexto(temp, "IndiceCluster.txt")

							fmt.Println(" O endereço(hash) ", clusters[index_cluster].Hash, " nao existe")
							return false, false, false
						}
					} else {
						fmt.Println("O endereço ", item2, " não foi encontrado na lista de inputs de outro cluster")
						fmt.Println("Indice do input atualizado")
						temp := []string{strconv.Itoa(index_input + 1)}
						Function2.EscreverTexto(temp, "IndiceInputCluster.txt")
						fmt.Println()
					}
				}
				temp := []string{strconv.Itoa(0)}
				Function2.EscreverTexto(temp, "IndiceInputCluster.txt")
				indiceInicialInput = 0
			} else {
				temp := []string{strconv.Itoa(0)}
				Function2.EscreverTexto(temp, "IndiceInputCluster.txt")
				indiceInicialInput = 0
				fmt.Println(" Cluster ignorado: ", HashAtual)
				fmt.Println("Tamanho desse cluster: ", tamanhoInput)
			}
			fmt.Println()
			fmt.Println("Atualizando Indice  para ", index_cluster+1)
			temp := []string{strconv.Itoa(index_cluster + 1)}
			Function2.EscreverTexto(temp, "IndiceCluster.txt")
			fmt.Println()
			tamanhoCluster = tamanhoCluster - 1
		}
		temp := []string{strconv.Itoa(0)}
		Function2.EscreverTexto(temp, "IndiceCluster.txt")
		return true, false, false
	}
	return true, false, false
}
