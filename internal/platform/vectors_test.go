package platform

import (
 "encoding/binary"
 "math"
 "strings"
 "testing"
)

func TestVectorEncoding(t *testing.T) {
 values:=make([]any,embeddingDimensions)
 for i:=range values{values[i]=float64(0)}
 values[0]=float64(1.25)
 encoded,err:=vectorBytes(values)
 if err!=nil{t.Fatal(err)}
 if len(encoded)!=384*4||math.Float32frombits(binary.LittleEndian.Uint32(encoded[:4]))!=1.25{t.Fatal("wrong Redis FLOAT32 representation")}
 values[1]=math.NaN()
 if _,err=vectorBytes(values);err==nil{t.Fatal("NaN accepted")}
 values[1]=float64(0);values[0]=float64(0)
 if _,err=vectorBytes(values);err==nil{t.Fatal("zero cosine vector accepted")}
 if _,err=vectorBytes(values[:10]);err==nil{t.Fatal("wrong dimensions accepted")}
}
func TestVectorFilterContainsOnlyAuthorizedGenerations(t *testing.T) {
 first:=documentVectorKey("private-document","generation-a")
 second:=documentVectorKey("shared-document","generation-b")
 query:=vectorQuery([]string{first,second},4)
 if strings.Contains(query,"private-document")||strings.Contains(query,"shared-document"){t.Fatal("raw IDs interpolated in Redis query")}
 if !strings.Contains(query,first+"|"+second)||!strings.Contains(query,"KNN 4")||!strings.Contains(query,"@embedding_key") {t.Fatal(query)}
 if first==documentVectorKey("private-document","generation-b"){t.Fatal("old generation can match the new index publication")}
}
