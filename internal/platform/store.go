package platform

import (
 "context"
 "encoding/json"
 "errors"
 "fmt"
 "time"

 "go.mongodb.org/mongo-driver/v2/bson"
 "go.mongodb.org/mongo-driver/v2/mongo"
 "go.mongodb.org/mongo-driver/v2/mongo/options"
 "go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

type Record struct {
 ID string `json:"id" bson:"_id"`
 Kind string `json:"kind" bson:"kind"`
 Owner string `json:"owner" bson:"owner_id"`
 Status string `json:"status" bson:"status"`
 Data map[string]any `json:"data" bson:"data"`
 Error string `json:"error,omitempty" bson:"error"`
 Created time.Time `json:"created_at" bson:"created_at"`
 Updated time.Time `json:"updated_at" bson:"updated_at"`
}

type AuditEvent struct {
 ID string `json:"id" bson:"_id"`
 Actor string `json:"actor" bson:"actor"`
 Action string `json:"action" bson:"action"`
 Target string `json:"target" bson:"target"`
 Data any `json:"data" bson:"data"`
 Created time.Time `json:"created_at" bson:"created_at"`
}

type KeyRecord struct {
 ID string `json:"id" bson:"_id"`
 Digest string `json:"-" bson:"digest"`
 Name string `json:"name" bson:"name"`
 Role string `json:"role" bson:"role"`
 Revoked bool `json:"revoked" bson:"revoked"`
 Created time.Time `json:"created_at" bson:"created_at"`
 Outbox []AuditEvent `json:"-" bson:"audit_outbox,omitempty"`
}

type Store struct { Client *mongo.Client; DB *mongo.Database; leaseToken string }

func OpenStore(ctx context.Context, uri, database string) (*Store,error) {
 client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(5*time.Second).
  SetBSONOptions(&options.BSONOptions{DefaultDocumentM:true}))
 if err != nil { return nil,err }
 if err = client.Ping(ctx,readpref.Primary()); err != nil { client.Disconnect(context.Background()); return nil,err }
 s := &Store{Client:client,DB:client.Database(database),leaseToken:token()}
 indexes := map[string][]mongo.IndexModel{
  "records": {
   {Keys:bson.D{{Key:"kind",Value:1},{Key:"created_at",Value:-1}}},
   {Keys:bson.D{{Key:"kind",Value:1},{Key:"status",Value:1},{Key:"created_at",Value:1}}},
   {Keys:bson.D{{Key:"owner_id",Value:1}}},
   {Keys:bson.D{{Key:"data.readers",Value:1}}},
   {Keys:bson.D{{Key:"kind",Value:1},{Key:"status",Value:1},{Key:"data.embedding_id",Value:1}}},
  },
  "api_keys": {{Keys:bson.D{{Key:"digest",Value:1}},Options:options.Index().SetUnique(true)}},
  "audit": {{Keys:bson.D{{Key:"created_at",Value:-1}}}},
  "request_metrics": {{Keys:bson.D{{Key:"created_at",Value:-1}}}},
 }
 for name, models := range indexes {
  if _,err = s.DB.Collection(name).Indexes().CreateMany(ctx,models); err != nil { client.Disconnect(context.Background()); return nil,err }
 }
 return s,nil
}

func (s *Store) Close(ctx context.Context) error { return s.Client.Disconnect(ctx) }
func (s *Store) Ping(ctx context.Context) error { return s.Client.Ping(ctx,readpref.Primary()) }
func normalizeRecord(row Record)(Record,error) {
 // Keep the public data model JSON-native; nested BSON maps/arrays otherwise have named Go types.
 bytes,err := json.Marshal(row.Data); if err != nil { return row,err }
 err = json.Unmarshal(bytes,&row.Data)
 if row.Data == nil { row.Data = map[string]any{} }
 return row,err
}
func (s *Store) Get(ctx context.Context,id string)(Record,error) {
 var row Record
 if err := s.DB.Collection("records").FindOne(ctx,bson.M{"_id":id}).Decode(&row); err != nil { return row,err }
 return normalizeRecord(row)
}
func (s *Store) Put(ctx context.Context,row Record) error { return s.put(ctx,row,nil) }
func (s *Store) PutAudited(ctx context.Context,row Record,actor,action string,data any) error {
 event:=newAudit(actor,action,row.ID,data); return s.put(ctx,row,&event)
}
func (s *Store) put(ctx context.Context,row Record,event *AuditEvent) error {
 now:=time.Now().UTC()
 update:=bson.M{"$set":bson.M{"status":row.Status,"data":row.Data,"error":row.Error,"updated_at":now},
  "$setOnInsert":bson.M{"kind":row.Kind,"owner_id":row.Owner,"created_at":now}}
 if event!=nil { update["$push"]=bson.M{"audit_outbox":event} }
 encoded,err:=bson.Marshal(update);if err!=nil{return err}
 if len(encoded)>15<<20{return errors.New("record exceeds safe MongoDB document size (15 MiB); reduce the upload")}
 _,err=s.DB.Collection("records").UpdateOne(ctx,bson.M{"_id":row.ID},update,options.UpdateOne().SetUpsert(true));return err
}
func(s *Store) List(ctx context.Context,kind string)([]Record,error) {
 cursor,err:=s.DB.Collection("records").Find(ctx,bson.M{"kind":kind},options.Find().SetSort(bson.D{{Key:"created_at",Value:-1},{Key:"_id",Value:1}}).SetLimit(500))
 if err!=nil{return nil,err};defer cursor.Close(ctx)
 rows:=[]Record{}
 for cursor.Next(ctx){var row Record;if err=cursor.Decode(&row);err!=nil{return nil,err};row,err=normalizeRecord(row);if err!=nil{return nil,err};rows=append(rows,row)}
 return rows,cursor.Err()
}
func(s *Store) Count(ctx context.Context,kind string)(int64,error){return s.DB.Collection("records").CountDocuments(ctx,bson.M{"kind":kind})}
func newAudit(actor,action,target string,data any)AuditEvent{return AuditEvent{ID:token()[:24],Actor:actor,Action:action,Target:target,Data:data,Created:time.Now().UTC()}}
func(s *Store) Audit(ctx context.Context,actor,action,target string,data any)error{_,err:=s.DB.Collection("audit").InsertOne(ctx,newAudit(actor,action,target,data));return err}

// Sensitive mutations append their audit event in the same document update.
// The outbox works on standalone MongoDB: no replica set or cross-document transaction is required.
func(s *Store) FlushAudit(ctx context.Context)error{
 for _,name:=range []string{"records","api_keys"}{
  collection:=s.DB.Collection(name)
  cursor,err:=collection.Find(ctx,bson.M{"audit_outbox.0":bson.M{"$exists":true}},options.Find().SetProjection(bson.M{"audit_outbox":1}).SetLimit(100))
  if err!=nil{return err}
  for cursor.Next(ctx){var source struct{ID string `bson:"_id"`;Events []AuditEvent `bson:"audit_outbox"`};if err=cursor.Decode(&source);err!=nil{cursor.Close(ctx);return err}
   for _,event:=range source.Events{
    _,err=s.DB.Collection("audit").InsertOne(ctx,event)
    if err!=nil&&!mongo.IsDuplicateKeyError(err){cursor.Close(ctx);return err}
    if _,err=collection.UpdateOne(ctx,bson.M{"_id":source.ID},bson.M{"$pull":bson.M{"audit_outbox":bson.M{"_id":event.ID}}});err!=nil{cursor.Close(ctx);return err}
   }
  }
  err=cursor.Err();cursor.Close(ctx);if err!=nil{return err}
 }
 return nil
}
func(s *Store) AuditList(ctx context.Context)([]AuditEvent,error){if err:=s.FlushAudit(ctx);err!=nil{return nil,err};cursor,err:=s.DB.Collection("audit").Find(ctx,bson.M{},options.Find().SetSort(bson.D{{Key:"created_at",Value:-1}}).SetLimit(500));if err!=nil{return nil,err};defer cursor.Close(ctx);out:=[]AuditEvent{};err=cursor.All(ctx,&out);return out,err}

func(s *Store) AcquireLease(ctx context.Context)error{
 now:=time.Now().UTC()
 filter:=bson.M{"_id":"gateway","$or":bson.A{bson.M{"expires_at":bson.M{"$lte":now}},bson.M{"owner":s.leaseToken}}}
 _,err:=s.DB.Collection("leases").UpdateOne(ctx,filter,bson.M{"$set":bson.M{"owner":s.leaseToken,"expires_at":now.Add(90*time.Second)}},options.UpdateOne().SetUpsert(true))
 if mongo.IsDuplicateKeyError(err){return errors.New("another gateway owns the scheduler; after an unclean stop its lease expires within 90 seconds")};return err
}
func(s *Store) RenewLease(ctx context.Context)error{now:=time.Now().UTC();result,err:=s.DB.Collection("leases").UpdateOne(ctx,bson.M{"_id":"gateway","owner":s.leaseToken,"expires_at":bson.M{"$gt":now}},bson.M{"$set":bson.M{"expires_at":now.Add(90*time.Second)}});if err!=nil{return err};if result.MatchedCount!=1{return errors.New("scheduler lease lost")};return nil}
func(s *Store) ReleaseLease(ctx context.Context)error{_,err:=s.DB.Collection("leases").DeleteOne(ctx,bson.M{"_id":"gateway","owner":s.leaseToken});return err}

func(s *Store) PendingJobs(ctx context.Context)(int64,error){return s.DB.Collection("records").CountDocuments(ctx,bson.M{"kind":"job","status":bson.M{"$in":[]string{"queued","running"}}})}
func(s *Store) ClaimJob(ctx context.Context)(Record,error){var row Record;err:=s.DB.Collection("records").FindOneAndUpdate(ctx,bson.M{"kind":"job","status":"queued"},bson.M{"$set":bson.M{"status":"running","updated_at":time.Now().UTC()}},options.FindOneAndUpdate().SetSort(bson.D{{Key:"created_at",Value:1},{Key:"_id",Value:1}}).SetReturnDocument(options.After)).Decode(&row);if err!=nil{return row,err};return normalizeRecord(row)}
func(s *Store) Recover(ctx context.Context)error{
 records:=s.DB.Collection("records")
 updates:=[]struct{filter bson.M;status,reason string}{
  {bson.M{"kind":"job","status":"running"},"interrupted","Gateway restarted during execution; submit a new job or resume a checkpoint"},
  {bson.M{"kind":"model","status":bson.M{"$in":[]string{"running","starting","stopping","downloading"}}},"stopped","Gateway restarted; start explicitly to reconcile the AI service"},
  {bson.M{"kind":"run","status":"running"},"interrupted","Agent interrupted by restart"},
 }
 for _,update:=range updates{if _,err:=records.UpdateMany(ctx,update.filter,bson.M{"$set":bson.M{"status":update.status,"error":update.reason,"updated_at":time.Now().UTC()}});err!=nil{return err}}
 cursor,err:=records.Find(ctx,bson.M{"kind":"document","status":"processing"});if err!=nil{return err};defer cursor.Close(ctx)
 for cursor.Next(ctx){var doc Record;if err=cursor.Decode(&doc);err!=nil{return err};count,e:=records.CountDocuments(ctx,bson.M{"kind":"job","status":"queued","data.operation":"ingest","data.payload.document":doc.ID});if e!=nil{return e};if count==0{doc.Status="failed";doc.Error="Ingestion interrupted by restart; upload again";if e=s.Put(ctx,doc);e!=nil{return e}}}
 return cursor.Err()
}
func(s *Store) Access(ctx context.Context,id,kind string,u User,write bool)(Record,error){row,err:=s.Get(ctx,id);if err!=nil||row.Kind!=kind||!readable(row,u){return row,errNotFound};if write&&u.Role!="administrator"&&row.Owner!=u.ID{return row,errForbidden};return row,nil}
func readable(row Record,u User)bool{if row.Kind=="model"||u.Role=="administrator"||row.Owner==u.ID{return true};for _,v:=range array(row.Data["readers"]){if v==u.ID{return true}};return false}
var errNotFound=errors.New("resource not found")
var errForbidden=errors.New("insufficient permissions")
var errConflict=errors.New("state already resolved")
func array(v any)[]any{if a,ok:=v.([]any);ok{return a};if a,ok:=v.([]string);ok{out:=make([]any,len(a));for i,s:=range a{out[i]=s};return out};return nil}
func str(v any)string{s,_:=v.(string);return s}
func number(v any)float64{switch n:=v.(type){case float64:return n;case int:return float64(n);case int32:return float64(n);case int64:return float64(n)};return 0}

func(s *Store) KeyByDigest(ctx context.Context,digest string)(User,error){var key KeyRecord;err:=s.DB.Collection("api_keys").FindOne(ctx,bson.M{"digest":digest,"revoked":false}).Decode(&key);return User{ID:key.ID,Role:key.Role,Name:key.Name},err}
func(s *Store) KeyByID(ctx context.Context,id string)(User,error){var key KeyRecord;err:=s.DB.Collection("api_keys").FindOne(ctx,bson.M{"_id":id,"revoked":false}).Decode(&key);return User{ID:key.ID,Role:key.Role,Name:key.Name},err}
func(s *Store) Keys(ctx context.Context)([]KeyRecord,error){cursor,err:=s.DB.Collection("api_keys").Find(ctx,bson.M{},options.Find().SetSort(bson.D{{Key:"created_at",Value:-1}}));if err!=nil{return nil,err};defer cursor.Close(ctx);out:=[]KeyRecord{};err=cursor.All(ctx,&out);return out,err}
func(s *Store) CreateKey(ctx context.Context,actor string,key KeyRecord)error{key.Created=time.Now().UTC();key.Outbox=[]AuditEvent{newAudit(actor,"key.create",key.ID,nil)};_,err:=s.DB.Collection("api_keys").InsertOne(ctx,key);return err}
func(s *Store) RevokeKey(ctx context.Context,id,actor string)error{result,err:=s.DB.Collection("api_keys").UpdateOne(ctx,bson.M{"_id":id},bson.M{"$set":bson.M{"revoked":true},"$push":bson.M{"audit_outbox":newAudit(actor,"key.revoke",id,nil)}});if err!=nil{return err};if result.MatchedCount==0{return errNotFound};return nil}
func(s *Store) SetReaders(ctx context.Context,id string,readers []string,actor,action string)error{if readers==nil{readers=[]string{}};_,err:=s.DB.Collection("records").UpdateOne(ctx,bson.M{"_id":id},bson.M{"$set":bson.M{"data.readers":readers,"updated_at":time.Now().UTC()},"$push":bson.M{"audit_outbox":newAudit(actor,action,id,map[string]any{"readers":readers})}});return err}
func(s *Store) ResolveApproval(ctx context.Context,id,actor string,approve bool)(Record,error){status:="rejected";if approve{status="running"};var row Record;event:=newAudit(actor,"agent.approval",id,map[string]any{"approve":approve});err:=s.DB.Collection("records").FindOneAndUpdate(ctx,bson.M{"_id":id,"kind":"run","status":"awaiting_approval"},bson.M{"$set":bson.M{"status":status,"data.approved_by":actor,"data.approved":approve,"updated_at":time.Now().UTC()},"$push":bson.M{"audit_outbox":event}},options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&row);if errors.Is(err,mongo.ErrNoDocuments){return row,errConflict};if err!=nil{return row,fmt.Errorf("persist approval: %w",err)};return normalizeRecord(row)}

func(s *Store) DocumentSource(ctx context.Context,id string)(map[string]any,error){
 var job Record
 err:=s.DB.Collection("records").FindOne(ctx,bson.M{"kind":"job","data.operation":"ingest","data.payload.document":id},options.FindOne().SetSort(bson.D{{Key:"created_at",Value:-1}})).Decode(&job)
 if err!=nil{return nil,err}
 job,err=normalizeRecord(job);if err!=nil{return nil,err}
 payload,ok:=job.Data["payload"].(map[string]any)
 if !ok||str(payload["content"])==""{return nil,errors.New("original document source unavailable")}
 return payload,nil
}

func(s *Store) SetStatus(ctx context.Context,id,status,reason string)error{
 result,err:=s.DB.Collection("records").UpdateOne(ctx,bson.M{"_id":id},bson.M{"$set":bson.M{"status":status,"error":reason,"updated_at":time.Now().UTC()}})
 if err!=nil{return err};if result.MatchedCount==0{return errNotFound};return nil
}
