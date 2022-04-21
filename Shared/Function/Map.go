package Function

func RemoveRepetidos(m1, m2 map[string]string) (map[string]string, int) {
	unionMap := []map[string]string{m1, m2}
	r := map[string]string{}

	for _, mapas := range unionMap {
		for endereco, _ := range mapas {
			_, ok := r[endereco]
			if ok {
				continue
			} else {
				r[endereco] = ""
			}
		}
	}

	return r, len(r)
}

func VerificaIgualdade(map_endereco_atual, map_enderecos_resultante map[string]string) bool {
	_, tamanho := RemoveRepetidos(map_endereco_atual, map_enderecos_resultante)

	if tamanho == len(map_endereco_atual) && tamanho == len(map_enderecos_resultante) {
		return true
	} else {
		return false
	}
}
