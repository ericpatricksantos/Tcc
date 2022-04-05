package Model

type Cluster struct {
	//Id    string   `json:"id" bson:"_id"`
	Hash  string   `json:"Hash"`
	Input []string `json:"Input"`
}
