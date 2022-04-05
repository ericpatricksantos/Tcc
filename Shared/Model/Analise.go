package Model

/*
	AddressNivel0 (Nivel 0 - Endereco Farao)
		|
	AddressAnterior
		|
	AddressInput
*/
type Distancia struct {
	AddressInput    string `json:"addressInput"`
	Distancia       int    `json:"distancia"` // distancia(niveis) do AddressNivel0
	Baixou          bool   `json:"baixou"`
	AddressNivel0   string `json:"addressNivel0"`   // endereco do farao AddressAnalisado (Nivel 0)
	AddressAnterior string `json:"addressAnterior"` // endereco onde foi retirado o AddressInput
}

type AddressAnalisado struct {
	Address string `json:"address"`
}
