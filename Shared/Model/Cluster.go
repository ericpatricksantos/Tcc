package Model

type Cluster struct {
	//Id    string   `json:"id" bson:"_id"`
	Hash  string   `json:"Hash"`
	Input []string `json:"Input"`
}

type Clusters struct {
	Identificador string   `json:"Identificador"`
	Clusters      []string `json:"Clusters"`
}

type Identificador struct {
	Identificador  string `json:"Identificador"`
	TamanhoCluster int    `json:"TamanhoCluster"`
}

type MapCluster struct {
	Identificador string            `json:"Identificador"`
	Clusters      map[string]string `json:"Clusters"`
}
