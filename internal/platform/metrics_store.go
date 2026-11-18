package platform

import (
 "context"
 "time"

 "go.mongodb.org/mongo-driver/v2/bson"
 "go.mongodb.org/mongo-driver/v2/mongo"
 "go.mongodb.org/mongo-driver/v2/mongo/options"
)

type RequestMetric struct {
 ID string `json:"id" bson:"_id"`
 Actor string `json:"actor" bson:"actor"`
 Model string `json:"model" bson:"model"`
 Status int `json:"status" bson:"status"`
 QueueMS float64 `json:"queue_ms" bson:"queue_ms"`
 TTFTMS *float64 `json:"ttft_ms" bson:"ttft_ms"`
 DurationMS float64 `json:"duration_ms" bson:"duration_ms"`
 OutputTokens int `json:"output_tokens" bson:"output_tokens"`
 Throughput *float64 `json:"tokens_per_second" bson:"throughput"`
 Created time.Time `json:"created_at" bson:"created_at"`
}

type RequestTotals struct {
 Total int64 `bson:"total"`
 Failed int64 `bson:"failed"`
 Tokens int64 `bson:"tokens"`
 QueueMS float64 `bson:"queue_ms"`
 DurationMS float64 `bson:"duration_ms"`
}

func(s *Store) SaveRequest(ctx context.Context, metric RequestMetric)error {
 metric.Created=time.Now().UTC()
 _,err:=s.DB.Collection("request_metrics").InsertOne(ctx,metric)
 return err
}
func(s *Store) Requests(ctx context.Context)([]RequestMetric,error){
 cursor,err:=s.DB.Collection("request_metrics").Find(ctx,bson.M{},options.Find().SetSort(bson.D{{Key:"created_at",Value:-1}}).SetLimit(500))
 if err!=nil{return nil,err};defer cursor.Close(ctx)
 out:=[]RequestMetric{};err=cursor.All(ctx,&out);return out,err
}
func(s *Store) RequestTotals(ctx context.Context)(RequestTotals,error){
 group:=bson.M{"_id":nil,"total":bson.M{"$sum":1},
  "failed":bson.M{"$sum":bson.M{"$cond":bson.A{bson.M{"$gte":bson.A{"$status",400}},1,0}}},
  "tokens":bson.M{"$sum":"$output_tokens"},"queue_ms":bson.M{"$sum":"$queue_ms"},"duration_ms":bson.M{"$sum":"$duration_ms"}}
 cursor,err:=s.DB.Collection("request_metrics").Aggregate(ctx,mongo.Pipeline{bson.D{{Key:"$group",Value:group}}})
 if err!=nil{return RequestTotals{},err};defer cursor.Close(ctx)
 var out RequestTotals
 if cursor.Next(ctx){if err=cursor.Decode(&out);err!=nil{return out,err}}
 return out,cursor.Err()
}
