package Model

type Informacoes struct {
	InfoEnderecos InfoEnderecos `json:"InfoEndereco"`
	InfoTransacoes InfoTransacoes `json:"InfoTransacoes"`
}

type InfoEnderecos struct {
	QtdEnderecos  int   `json:"QtdEnderecos"`
	Enderecos []string `json:"Enderecos"`
}

type InfoTransacoes struct {
	QtdTransacoes  int   `json:"QtdTransacoes"`
	Transacoes []string `json:"Transacoes"`
}
