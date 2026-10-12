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
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}

	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		return nil, err
	}
	if value == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, nil
	}

	resp := &kvrpcpb.RawGetResponse{
		Value: value,
	}

	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	m := storage.Modify{
		Data: storage.Put{
			Key:   req.GetKey(),
			Value: req.GetValue(),
			Cf:    req.GetCf(),
		},
	}
	err := server.storage.Write(req.GetContext(), []storage.Modify{m})
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	m := storage.Modify{
		Data: storage.Delete{
			Key: req.GetKey(),
			Cf:  req.GetCf(),
		},
	}
	err := server.storage.Write(req.GetContext(), []storage.Modify{m})
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		return nil, err
	}

	iter := reader.IterCF(req.GetCf())
	defer iter.Close()

	iter.Seek(req.GetStartKey())

	resp := &kvrpcpb.RawScanResponse{}
	limit := req.GetLimit()
	for iter.Valid() && limit > 0 {
		item := iter.Item()
		value, err := item.Value()
		if err != nil {
			return nil, err
		}

		resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: value,
		})

		iter.Next()
		limit--
	}

	return resp, nil
}
