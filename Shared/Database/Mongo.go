package Database

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// This is a user defined method to close resources.
// This method closes mongoDB connection and cancel context.
func Close(client *mongo.Client, ctx context.Context,
	cancel context.CancelFunc) {

	// CancelFunc to cancel to context
	defer cancel()

	// client provides a method to close
	// a mongoDB connection.
	defer func() {

		// client.Disconnect method also has deadline.
		// returns error if any,
		if err := client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

}

// This is a user defined method that returns mongo.Client,
// context.Context, context.CancelFunc and error.
// mongo.Client will be used for further database operation.
// context.Context will be used set deadlines for process.
// context.CancelFunc will be used to cancel context and
// resource associtated with it.

func Connect(uri string) (*mongo.Client, context.Context,
	context.CancelFunc, error) {

	// ctx will be used to set deadline for process, here
	// deadline will of 30 seconds.
	ctx, cancel := context.WithTimeout(context.Background(),
		300*time.Second)

	//ctx := context.Background()

	// mongo.Connect return mongo.Client method
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	return client, ctx, cancel, err
}

// This is a user defined method that accepts
// mongo.Client and context.Context
// This method used to ping the mongoDB, return error if any.
func Ping(client *mongo.Client, ctx context.Context) error {

	// mongo.Client has Ping to ping mongoDB, deadline of
	// the Ping method will be determined by cxt
	// Ping method return error if any occored, then
	// the error can be handled.
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return err
	}
	fmt.Println("connected successfully")
	return nil
}

// insertOne is a user defined method, used to insert
// documents into collection returns result of InsertOne
// and error if any.
func InsertOne(client *mongo.Client, ctx context.Context, dataBase, col string, doc interface{}) (*mongo.InsertOneResult, error) {

	// select database and collection ith Client.Database method
	// and Database.Collection method
	collection := client.Database(dataBase).Collection(col)

	// InsertOne accept two argument of type Context
	// and of empty interface
	result, err := collection.InsertOne(ctx, doc)
	return result, err
}


// inserMany is a user defined method, used to insert
// documents into collection returns result of
// InsertMany and error if any.
func InsertMany(client *mongo.Client, ctx context.Context, dataBase, col string, docs []interface{}) (*mongo.InsertManyResult, error) {

	// select database and collection ith Client.Database
	// method and Database.Collection method
	collection := client.Database(dataBase).Collection(col)

	// InsertMany accept two argument of type Context
	// and of empty interface
	result, err := collection.InsertMany(ctx, docs)
	return result, err
}

func Query(client *mongo.Client, ctx context.Context, dataBase, col string, query, field interface{}) (result *mongo.Cursor, err error) {

	// select database and collection.
	collection := client.Database(dataBase).Collection(col)

	// collection has an method Find,
	// that returns a mongo.cursor
	// based on query and field.
	//result, err = collection.Find(ctx, query, options.Find().SetProjection(field))
	opcao := options.Find()
	opcao.SetLimit(100)
	opcao.SetProjection(field)
	result, err = collection.Find(ctx, query, opcao)
	return result, err
}

func QueryLimit(client *mongo.Client, ctx context.Context, dataBase, col string, limit int64, query, field interface{}) (result *mongo.Cursor, err error) {

	// select database and collection.
	collection := client.Database(dataBase).Collection(col)

	// collection has an method Find,
	// that returns a mongo.cursor
	// based on query and field.
	//result, err = collection.Find(ctx, query, options.Find().SetProjection(field))
	opcao := options.Find()
	opcao.SetLimit(limit)
	opcao.SetProjection(field)
	result, err = collection.Find(ctx, query, opcao)
	return result, err
}

func QueryLimitOffset(client *mongo.Client, ctx context.Context, dataBase, col string, limit int64, offset int64, query, field interface{}) (result *mongo.Cursor, err error) {

	// select database and collection.
	collection := client.Database(dataBase).Collection(col)

	// collection has an method Find,
	// that returns a mongo.cursor
	// based on query and field.
	//result, err = collection.Find(ctx, query, options.Find().SetProjection(field))
	opcao := options.Find()
	opcao.SetLimit(limit)
	opcao.SetSkip(offset)
	opcao.SetProjection(field)
	result, err = collection.Find(ctx, query, opcao)
	return result, err
}

/*
Busca a ocorrencia de um elemente atráves de uma chave e um valor
	Exemplo:
			Key = _id , Code = "6153a58d3700e70e40f8177a"
			Key = adresses , Code = "13adwKvLLpHdcYDh21FguCdJgKhaYP3Dse"
	return
		 0 ocorrencia do docuemnto
		 1 ocorrencia do documento
		 2 ocorrencias do documento
*/
func CountElemento(client *mongo.Client, ctx context.Context, dataBase string, col string, key string, code string) (Count int64, err error) {

	// select database and collection.
	collection := client.Database(dataBase).Collection(col)

	var filter bson.M

	if key == "_id" {
		objectId, _ := primitive.ObjectIDFromHex("code")
		filter = bson.M{
			key: objectId,
		}
		opts := options.Count().SetMaxTime(2 * time.Second)
		Count, err = collection.CountDocuments(
			context.TODO(),
			filter,
			opts)
		if err != nil {
			log.Fatal(err)
		}

	} else {
		filter = bson.M{
			key: code,
		}

		opts := options.Count().SetMaxTime(2 * time.Second)
		Count, err = collection.CountDocuments(
			ctx,
			filter,
			opts)
		if err != nil {
			fmt.Println("Erro na Função CountElemento - {Database/Mongo.go}")
			log.Fatal(err)
		}
	}

	return Count, err
}

func CountElementoTxIndex(client *mongo.Client, ctx context.Context, dataBase, col string, key string, code int) (Count int64, err error) {

	// select database and collection.
	collection := client.Database(dataBase).Collection(col)

	var filter bson.M

	if key == "_id" {
		objectId, _ := primitive.ObjectIDFromHex("code")
		filter = bson.M{
			key: objectId,
		}
		opts := options.Count().SetMaxTime(2 * time.Second)
		Count, err = collection.CountDocuments(
			context.TODO(),
			filter,
			opts)
		if err != nil {
			log.Fatal(err)
		}

	} else {
		filter = bson.M{
			key: code,
		}

		opts := options.Count().SetMaxTime(2 * time.Second)
		Count, err = collection.CountDocuments(
			ctx,
			filter,
			opts)
		if err != nil {
			log.Fatal(err)
		}
	}

	return Count, err
}

func AdicionaAddress(client *mongo.Client, ctx context.Context, dataBase, col string, filter, update interface{}, opts *options.UpdateOptions) (result *mongo.UpdateResult, err error) {

	// select the databse and the collection
	collection := client.Database(dataBase).Collection(col)

	// A single document that match with the
	// filter will get updated.
	// update contains the filed which should get updated.
	result, err = collection.UpdateOne(ctx, filter, update, opts)
	return
}

// UpdateOne is a user defined method, that update
// a single document matching the filter.
// This methos accepts client, context, databse,
// collection, filter and update filter and update
// is of type interface this method returns
// UpdateResult and an error if any.
func UpdateOne(client *mongo.Client, ctx context.Context, dataBase, col string, filter, update interface{}) (result *mongo.UpdateResult, err error) {

	// select the databse and the collection
	collection := client.Database(dataBase).Collection(col)

	// A single document that match with the
	// filter will get updated.
	// update contains the filed which should get updated.
	result, err = collection.UpdateOne(ctx, filter, update)
	return
}

// UpdateMany is a user defined method, that update
// a multiple document matching the filter.
// This methos accepts client, context, databse,
// collection, filter and update filter and update
// is of type interface this method returns
// UpdateResult and an error if any.
func UpdateMany(client *mongo.Client, ctx context.Context, dataBase, col string, filter, update interface{}) (result *mongo.UpdateResult, err error) {

	// select the databse and the collection
	collection := client.Database(dataBase).Collection(col)

	// All the documents that match with the filter will
	// get updated.
	// update contains the filed which should get updated.
	result, err = collection.UpdateMany(ctx, filter, update)
	return
}

// deleteOne is a user defined function that delete,
// a single document from the collection.
// Returns DeleteResult and an  error if any.
func DeleteOne(client *mongo.Client, ctx context.Context, dataBase, col string, query interface{}) (result *mongo.DeleteResult, err error) {

	// select document and collection
	collection := client.Database(dataBase).Collection(col)

	// query is used to match a document  from the collection.
	result, err = collection.DeleteOne(ctx, query)
	return
}

// deleteMany is a user defined function that delete,
// multiple documents from the collection.
// Returns DeleteResult and an  error if any.
func DeleteMany(client *mongo.Client, ctx context.Context, dataBase, col string, query interface{}) (result *mongo.DeleteResult, err error) {

	// select document and collection
	collection := client.Database(dataBase).Collection(col)

	// query is used to match  documents  from the collection.
	result, err = collection.DeleteMany(ctx, query)
	return
}

func ToDoc(v interface{}) (doc *bson.Decoder, err error) {
	data, err := bson.Marshal(v)
	if err != nil {
		return
	}

	err = bson.Unmarshal(data, &doc)
	return
}
