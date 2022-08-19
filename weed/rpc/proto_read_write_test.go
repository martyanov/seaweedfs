package rpc

import (
	"fmt"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/seaweedfs/seaweedfs/weed/rpc/volume_server_pb"
)

func TestJsonpMarshalUnmarshal(t *testing.T) {

	tv := &volume_server_pb.RemoteFile{
		BackendType: "aws",
		BackendId:   "",
		FileSize:    12,
	}

	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
		Indent:          "  ",
	}

	if text, err := m.Marshal(tv); err != nil {
		fmt.Printf("marshal eror: %v\n", err)
	} else {
		fmt.Printf("marshalled: %s\n", string(text))
	}

	rawJson := `{
		"backendType":"aws",
		"backendId":"temp",
		"fileSize":12
	}`

	tv1 := &volume_server_pb.RemoteFile{}
	if err := protojson.Unmarshal([]byte(rawJson), tv1); err != nil {
		fmt.Printf("unmarshal error: %v\n", err)
	}

	fmt.Printf("unmarshalled: %+v\n", tv1)

}
