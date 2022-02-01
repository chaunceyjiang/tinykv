package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	resp := &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(nil)
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	rawValue, err := reader.GetCF(req.GetCf(), req.GetKey())

	if rawValue == nil && err == nil {
		resp.NotFound = true
	}
	resp.Value = rawValue
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified

	data := storage.Modify{Data: storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}}
	err := server.storage.Write(nil, []storage.Modify{data})
	resp := &kvrpcpb.RawPutResponse{}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	data := storage.Modify{Data: storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}}
	err := server.storage.Write(nil, []storage.Modify{data})
	resp := &kvrpcpb.RawDeleteResponse{}
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	resp := &kvrpcpb.RawScanResponse{}
	reader, err := server.storage.Reader(nil)
	if err != nil {
		resp.Error = err.Error()
		return resp, nil
	}
	dbIterator := reader.IterCF(req.GetCf())
	kvs := make([]*kvrpcpb.KvPair, 0)
	limit := uint32(0)

	for dbIterator.Seek(req.StartKey); dbIterator.Valid(); dbIterator.Next() {
		item := dbIterator.Item()
		v, err := item.Value()
		if err != nil {
			resp.Error = err.Error()
			return resp, nil
		}
		kv := &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: v,
		}
		kvs = append(kvs, kv)
		limit++
		if limit == req.Limit {
			break
		}
	}

	resp.Kvs = kvs
	return resp, nil
}
