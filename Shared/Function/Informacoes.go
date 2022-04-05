package Function

import (
	"Tcc/Shared/Database"
	"Tcc/Shared/Model"
	"gopkg.in/mgo.v2/bson"
	"log"
)

func SaveInfo(Addr Model.Informacoes, ConnectionMongoDB string, DataBaseMongo string, Collection string) bool {

		cliente, contexto, cancel, errou := Database.Connect(ConnectionMongoDB)
		if errou != nil {
			panic(errou)
		}

		Database.Ping(cliente, contexto)
		defer Database.Close(cliente, contexto, cancel)

		Database.ToDoc(Addr)

		result, err := Database.InsertOne(cliente, contexto, DataBaseMongo, Collection, Addr)
		// handle the error
		if err != nil {
			panic(err)
		}

		if result.InsertedID != nil {
			return true
		} else {
			return false
		}

}

func GetInfoMongoDB(ConnectionMongoDB string, DataBaseMongo string, CollectionRecuperaDados string) (addr Model.Informacoes) {
	// Get Client, Context, CalcelFunc and err from connect method.
	client, ctx, cancel, err := Database.Connect(ConnectionMongoDB)
	if err != nil {

		panic(err)
	}

	defer Database.Close(client, ctx, cancel)
	var filter, option interface{}

	filter = bson.M{}

	option = bson.M{}

	cursor, err := Database.Query(client, ctx, DataBaseMongo,
		CollectionRecuperaDados, filter, option)

	if err != nil {

		panic(err)
	}

	// le os documentos em partes, testei com 1000 documentos e deu certo
	defer cursor.Close(ctx)

	for cursor.Next(ctx) {

		if err := cursor.Decode(&addr); err != nil {
			log.Fatal(err)
		}

		return addr
	}

	return Model.Informacoes{}
}

