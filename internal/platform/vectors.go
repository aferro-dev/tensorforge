package platform

import (
 "context"
 "encoding/binary"
 "errors"
 "fmt"
 "math"
 "regexp"
 "sort"
 "strconv"
 "strings"
 "time"

 "github.com/redis/go-redis/v9"
 "go.mongodb.org/mongo-driver/v2/bson"
 "go.mongodb.org/mongo-driver/v2/mongo/options"
)

const embeddingDimensions = 384
const vectorBatchSize = 128
var namespacePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

type VectorStore struct { Client *redis.Client; Index, Prefix string }

func OpenVectorStore(ctx context.Context, uri, namespace string)(*VectorStore,error) {
 if !namespacePattern.MatchString(namespace) { return nil,errors.New("TF_REDIS_NAMESPACE must contain 1–64 letters, digits, underscores or hyphens") }
 config,err:=redis.ParseURL(uri);if err!=nil{return nil,err}
 config.Protocol=2
 config.DialTimeout=3*time.Second
 config.ReadTimeout=15*time.Second
 config.WriteTimeout=15*time.Second
 config.ContextTimeoutEnabled=true
 client:=redis.NewClient(config)
 vectors:=&VectorStore{Client:client,Index:namespace+":rag-v1",Prefix:namespace+":chunk:"}
 if err=client.Ping(ctx).Err();err==nil{err=vectors.ensureIndex(ctx)}
 if err!=nil{client.Close();return nil,fmt.Errorf("Redis vector search unavailable (Redis Search is required): %w",err)}
 return vectors,nil
}
func(v *VectorStore) ensureIndex(ctx context.Context)error {
 // FLAT is an actual Redis vector index and gives exact KNN for this single-node workload.
 err:=v.Client.Do(ctx,"FT.CREATE",v.Index,"ON","HASH","PREFIX",1,v.Prefix,"SCHEMA",
  "document_key","TAG","embedding_key","TAG","embedding","VECTOR","FLAT",6,
  "TYPE","FLOAT32","DIM",embeddingDimensions,"DISTANCE_METRIC","COSINE").Err()
 if err!=nil&&!strings.Contains(strings.ToLower(err.Error()),"index already exists"){return err}
 info,err:=v.Client.Do(ctx,"FT.INFO",v.Index).Result();if err!=nil{return err}
 definition:=redisPairs(redisPairs(info)["index_definition"])
 prefixes,_:=definition["prefixes"].([]any)
 if !strings.EqualFold(redisText(definition["key_type"]),"HASH")||len(prefixes)!=1||redisText(prefixes[0])!=v.Prefix{return errors.New("existing Redis index uses a different hash prefix; select a fresh namespace")}
 attributes,_:=redisPairs(info)["attributes"].([]any)
 foundDocument,foundModel,foundVector:=false,false,false
 for _,attribute:=range attributes{
  fields:=redisPairs(attribute)
  name:=redisText(fields["attribute"])
  switch name {
  case "document_key":foundDocument=strings.EqualFold(redisText(fields["type"]),"TAG")
  case "embedding_key":foundModel=strings.EqualFold(redisText(fields["type"]),"TAG")
  case "embedding":foundVector=strings.EqualFold(redisText(fields["type"]),"VECTOR")&&
    strings.EqualFold(redisText(fields["data_type"]),"FLOAT32")&&redisText(fields["dim"])=="384"&&
    strings.EqualFold(redisText(fields["distance_metric"]),"COSINE")&&strings.EqualFold(redisText(fields["algorithm"]),"FLAT")
  }
 }
 if !foundDocument||!foundModel||!foundVector{return errors.New("existing Redis index has an incompatible schema; use a fresh namespace and reindex")}
 return nil
}
func redisPairs(value any)map[string]any{out:=map[string]any{};list,ok:=value.([]any);if !ok{return out};for i:=0;i+1<len(list);i+=2{out[redisText(list[i])]=list[i+1]};return out}
func redisText(value any)string{switch v:=value.(type){case string:return v;case []byte:return string(v);case nil:return "";default:return fmt.Sprint(v)}}

func vectorBytes(values []any)([]byte,error){
 if len(values)!=embeddingDimensions{return nil,fmt.Errorf("embedding must have %d dimensions",embeddingDimensions)}
 output:=make([]byte,embeddingDimensions*4);norm:=0.0
 for i,value:=range values{
  n,ok:=value.(float64)
  if !ok||math.IsNaN(n)||math.IsInf(n,0)||math.Abs(n)>math.MaxFloat32{return nil,errors.New("embedding contains an invalid float32 value")}
  converted:=float32(n)
  norm+=float64(converted)*float64(converted)
  binary.LittleEndian.PutUint32(output[i*4:],math.Float32bits(converted))
 }
 if norm==0||math.IsInf(norm,0){return nil,errors.New("cosine embedding must have a finite nonzero norm")}
 return output,nil
}
func documentVectorKey(id,generation string)string{return hash(id+"@"+generation)}

func(a *App) saveChunks(ctx context.Context,doc Record,result map[string]any)error {
 chunks:=array(result["chunks"]);embeddingID:=str(result["embedding_id"])
 if len(chunks)==0||len(chunks)>2000||embeddingID==""{return errors.New("invalid document chunks or missing embedding provenance")}
 generation:=token()[:24]
 type pending struct{key string;fields map[string]any}
 writes:=make([]pending,0,len(chunks))
 for ordinal,item:=range chunks{
  obj,ok:=item.(map[string]any);if !ok{return errors.New("invalid chunk")}
  vector,err:=vectorBytes(array(obj["embedding"]));if err!=nil{return err}
  content:=str(obj["text"]);if content==""||len(content)>16000{return errors.New("empty or oversized chunk")}
  id:=token()[:24]
  writes=append(writes,pending{key:a.vectors.Prefix+doc.ID+":"+generation+":"+id,fields:map[string]any{
   "id":id,"document_id":doc.ID,"document_key":documentVectorKey(doc.ID,generation),
   "embedding_key":hash(embeddingID),"ordinal":ordinal,"content":content,"embedding":vector,
  }})
 }
 // Staged chunks cannot match any authorized document generation until MongoDB publishes it.
 _,err:=a.vectors.Client.TxPipelined(ctx,func(pipe redis.Pipeliner)error{
  for _,write:=range writes{pipe.HSet(ctx,write.key,write.fields);pipe.Expire(ctx,write.key,time.Hour)}
  return nil
 })
 if err==nil{
  _,err=a.vectors.Client.TxPipelined(ctx,func(pipe redis.Pipeliner)error{for _,write:=range writes{pipe.Persist(ctx,write.key)};return nil})
 }
 if err!=nil{
  cleanup,done:=context.WithTimeout(context.Background(),3*time.Second);defer done()
  keys:=make([]string,len(writes));for i,write:=range writes{keys[i]=write.key};a.vectors.Client.Del(cleanup,keys...)
  return fmt.Errorf("index document in Redis: %w",err)
 }
 doc.Data["rag_generation"]=generation
 doc.Data["embedding_id"]=embeddingID
 doc.Data["chunk_count"]=len(chunks)
 return nil
}

func(s *Store) AuthorizedDocuments(ctx context.Context,user User,embeddingID string)([]Record,error){
 filter:=bson.M{"kind":"document","status":"ready","data.embedding_id":embeddingID,"data.rag_generation":bson.M{"$exists":true}}
 if user.Role!="administrator"{filter["$or"]=bson.A{bson.M{"owner_id":user.ID},bson.M{"data.readers":user.ID}}}
 projection:=bson.M{"_id":1,"kind":1,"owner_id":1,"status":1,"data.name":1,"data.rag_generation":1,"data.embedding_id":1,"data.readers":1}
 cursor,err:=s.DB.Collection("records").Find(ctx,filter,options.Find().SetProjection(projection));if err!=nil{return nil,err};defer cursor.Close(ctx)
 out:=[]Record{};for cursor.Next(ctx){var row Record;if err=cursor.Decode(&row);err!=nil{return nil,err};row,err=normalizeRecord(row);if err!=nil{return nil,err};out=append(out,row)}
 return out,cursor.Err()
}
func vectorQuery(tags []string,k int)string{
 // Tags are SHA-256 hex digests produced locally, never raw user query text.
 return fmt.Sprintf("(@embedding_key:{$embedding_model} @document_key:{%s})=>[KNN %d @embedding $query_vector AS distance]",strings.Join(tags,"|"),k)
}
func(v *VectorStore) Search(ctx context.Context,documents []Record,embeddingID string,vector []byte,k int)([]Passage,error){
 if len(vector)!=embeddingDimensions*4||k<1||k>12{return nil,errors.New("invalid vector search parameters")}
 out:=[]Passage{}
 for offset:=0;offset<len(documents);offset+=vectorBatchSize{
  end:=min(offset+vectorBatchSize,len(documents));tags:=[]string{};allowed:=map[string]Record{}
  for _,doc:=range documents[offset:end]{tag:=documentVectorKey(doc.ID,str(doc.Data["rag_generation"]));tags=append(tags,tag);allowed[tag]=doc}
  raw,err:=v.Client.Do(ctx,"FT.SEARCH",v.Index,vectorQuery(tags,k),"PARAMS",4,"embedding_model",hash(embeddingID),"query_vector",vector,
   "SORTBY","distance","ASC","RETURN",6,"id","document_id","document_key","ordinal","content","distance","LIMIT",0,k,"DIALECT",2).Result()
  if err!=nil{return nil,fmt.Errorf("Redis KNN search: %w",err)}
  reply,ok:=raw.([]any);if !ok||len(reply)==0{return nil,errors.New("malformed Redis search response")}
  for i:=1;i+1<len(reply);i+=2{
   fields:=redisPairs(reply[i+1]);doc,authorized:=allowed[redisText(fields["document_key"])]
   if !authorized||redisText(fields["document_id"])!=doc.ID{return nil,errors.New("Redis returned a chunk outside the authorized document generation")}
   ordinal,err:=strconv.Atoi(redisText(fields["ordinal"]));if err!=nil{return nil,errors.New("invalid chunk ordinal")}
   distance,err:=strconv.ParseFloat(redisText(fields["distance"]),64);if err!=nil||math.IsNaN(distance)||math.IsInf(distance,0){return nil,errors.New("invalid vector distance")}
   passage:=Passage{ID:redisText(fields["id"]),Document:doc.ID,Name:str(doc.Data["name"]),Ordinal:ordinal,Text:redisText(fields["content"]),Score:1-distance}
   out=append(out,passage)
  }
 }
 sort.Slice(out,func(i,j int)bool{if out[i].Score==out[j].Score{return out[i].ID<out[j].ID};return out[i].Score>out[j].Score})
 if len(out)>k{out=out[:k]}
 return out,nil
}
