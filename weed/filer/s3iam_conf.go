package filer

import (
	"fmt"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/rpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func ParseS3ConfigurationFromBytes[T proto.Message](content []byte, config T) error {
	if err := protojson.Unmarshal(content, config); err != nil {
		return err
	}
	return nil
}

func ProtoToText(writer io.Writer, config proto.Message) error {

	m := protojson.MarshalOptions{
		EmitUnpopulated: true,
		Indent:          "  ",
	}

	text, marshalErr := m.Marshal(config)
	if marshalErr != nil {
		return fmt.Errorf("marshal proto message: %v", marshalErr)
	}

	_, writeErr := writer.Write(text)
	if writeErr != nil {
		return fmt.Errorf("fail to write proto message: %v", writeErr)
	}

	return writeErr
}

// CheckDuplicateAccessKey returns an error message when s3cfg has duplicate access keys
func CheckDuplicateAccessKey(s3cfg *rpc.IAMConfiguration) error {
	accessKeySet := make(map[string]string)
	for _, ident := range s3cfg.Identities {
		for _, cred := range ident.Credentials {
			if userName, found := accessKeySet[cred.AccessKey]; !found {
				accessKeySet[cred.AccessKey] = ident.Name
			} else {
				return fmt.Errorf("duplicate accessKey[%s], already configured in user[%s]", cred.AccessKey, userName)
			}
		}
	}
	return nil
}
