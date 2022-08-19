package iamapi

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/rpc/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"

	"github.com/aws/aws-sdk-go/service/iam"
)

const (
	charsetUpper           = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	charset                = charsetUpper + "abcdefghijklmnopqrstuvwxyz/"
	policyDocumentVersion  = "2012-10-17"
	StatementActionAdmin   = "*"
	StatementActionWrite   = "Put*"
	StatementActionRead    = "Get*"
	StatementActionList    = "List*"
	StatementActionTagging = "Tagging*"
)

var (
	seededRand *rand.Rand = rand.New(
		rand.NewSource(time.Now().UnixNano()))
	policyDocuments = map[string]*PolicyDocument{}
	policyLock      = sync.RWMutex{}
)

func MapToStatementAction(action string) string {
	switch action {
	case StatementActionAdmin:
		return s3_constants.ACTION_ADMIN
	case StatementActionWrite:
		return s3_constants.ACTION_WRITE
	case StatementActionRead:
		return s3_constants.ACTION_READ
	case StatementActionList:
		return s3_constants.ACTION_LIST
	case StatementActionTagging:
		return s3_constants.ACTION_TAGGING
	default:
		return ""
	}
}

func MapToIdentitiesAction(action string) string {
	switch action {
	case s3_constants.ACTION_ADMIN:
		return StatementActionAdmin
	case s3_constants.ACTION_WRITE:
		return StatementActionWrite
	case s3_constants.ACTION_READ:
		return StatementActionRead
	case s3_constants.ACTION_LIST:
		return StatementActionList
	case s3_constants.ACTION_TAGGING:
		return StatementActionTagging
	default:
		return ""
	}
}

type Statement struct {
	Effect   string   `json:"Effect"`
	Action   []string `json:"Action"`
	Resource []string `json:"Resource"`
}

type Policies struct {
	Policies map[string]PolicyDocument `json:"policies"`
}

type PolicyDocument struct {
	Version   string       `json:"Version"`
	Statement []*Statement `json:"Statement"`
}

func (p PolicyDocument) String() string {
	b, _ := json.Marshal(p)
	return string(b)
}

func Hash(s *string) string {
	h := sha1.New()
	h.Write([]byte(*s))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func (iama *IamApiServer) ListUsers(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp ListUsersResponse) {
	for _, ident := range s3cfg.Identities {
		resp.ListUsersResult.Users = append(resp.ListUsersResult.Users, &iam.User{UserName: &ident.Name})
	}
	return resp
}

func (iama *IamApiServer) ListAccessKeys(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp ListAccessKeysResponse) {
	status := iam.StatusTypeActive
	userName := values.Get("UserName")
	for _, ident := range s3cfg.Identities {
		if userName != "" && userName != ident.Name {
			continue
		}
		for _, cred := range ident.Credentials {
			resp.ListAccessKeysResult.AccessKeyMetadata = append(resp.ListAccessKeysResult.AccessKeyMetadata,
				&iam.AccessKeyMetadata{UserName: &ident.Name, AccessKeyId: &cred.AccessKey, Status: &status},
			)
		}
	}
	return resp
}

func (iama *IamApiServer) CreateUser(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp CreateUserResponse) {
	userName := values.Get("UserName")
	resp.CreateUserResult.User.UserName = &userName
	s3cfg.Identities = append(s3cfg.Identities, &iam_pb.Identity{Name: userName})
	return resp
}

func (iama *IamApiServer) DeleteUser(s3cfg *iam_pb.S3ApiConfiguration, userName string) (resp DeleteUserResponse, err error) {
	for i, ident := range s3cfg.Identities {
		if userName == ident.Name {
			s3cfg.Identities = append(s3cfg.Identities[:i], s3cfg.Identities[i+1:]...)
			return resp, nil
		}
	}
	return resp, fmt.Errorf(iam.ErrCodeNoSuchEntityException)
}

func (iama *IamApiServer) GetUser(s3cfg *iam_pb.S3ApiConfiguration, userName string) (resp GetUserResponse, err error) {
	for _, ident := range s3cfg.Identities {
		if userName == ident.Name {
			resp.GetUserResult.User = iam.User{UserName: &ident.Name}
			return resp, nil
		}
	}
	return resp, fmt.Errorf(iam.ErrCodeNoSuchEntityException)
}

func (iama *IamApiServer) UpdateUser(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp UpdateUserResponse, err error) {
	userName := values.Get("UserName")
	newUserName := values.Get("NewUserName")
	if newUserName != "" {
		for _, ident := range s3cfg.Identities {
			if userName == ident.Name {
				ident.Name = newUserName
				return resp, nil
			}
		}
	} else {
		return resp, nil
	}
	return resp, fmt.Errorf(iam.ErrCodeNoSuchEntityException)
}

func GetPolicyDocument(policy *string) (policyDocument PolicyDocument, err error) {
	if err = json.Unmarshal([]byte(*policy), &policyDocument); err != nil {
		return PolicyDocument{}, err
	}
	return policyDocument, err
}

func (iama *IamApiServer) CreatePolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp CreatePolicyResponse, err error) {
	policyName := values.Get("PolicyName")
	policyDocumentString := values.Get("PolicyDocument")
	policyDocument, err := GetPolicyDocument(&policyDocumentString)
	if err != nil {
		return CreatePolicyResponse{}, err
	}
	policyId := Hash(&policyDocumentString)
	arn := fmt.Sprintf("arn:aws:iam:::policy/%s", policyName)
	resp.CreatePolicyResult.Policy.PolicyName = &policyName
	resp.CreatePolicyResult.Policy.Arn = &arn
	resp.CreatePolicyResult.Policy.PolicyId = &policyId
	policies := Policies{}
	policyLock.Lock()
	defer policyLock.Unlock()
	if err = iama.s3ApiConfig.GetPolicies(&policies); err != nil {
		return resp, err
	}
	policies.Policies[policyName] = policyDocument
	if err = iama.s3ApiConfig.PutPolicies(&policies); err != nil {
		return resp, err
	}
	return resp, nil
}

// https://docs.aws.amazon.com/IAM/latest/APIReference/API_PutUserPolicy.html
func (iama *IamApiServer) PutUserPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp PutUserPolicyResponse, err error) {
	userName := values.Get("UserName")
	policyName := values.Get("PolicyName")
	policyDocumentString := values.Get("PolicyDocument")
	policyDocument, err := GetPolicyDocument(&policyDocumentString)
	if err != nil {
		return PutUserPolicyResponse{}, err
	}
	policyDocuments[policyName] = &policyDocument
	actions := GetActions(&policyDocument)
	for _, ident := range s3cfg.Identities {
		if userName != ident.Name {
			continue
		}
		ident.Actions = actions
		return resp, nil
	}
	return resp, fmt.Errorf("%s: the user with name %s cannot be found", iam.ErrCodeNoSuchEntityException, userName)
}

func (iama *IamApiServer) GetUserPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp GetUserPolicyResponse, err error) {
	userName := values.Get("UserName")
	policyName := values.Get("PolicyName")
	for _, ident := range s3cfg.Identities {
		if userName != ident.Name {
			continue
		}

		resp.GetUserPolicyResult.UserName = userName
		resp.GetUserPolicyResult.PolicyName = policyName
		if len(ident.Actions) == 0 {
			return resp, fmt.Errorf(iam.ErrCodeNoSuchEntityException)
		}

		policyDocument := PolicyDocument{Version: policyDocumentVersion}
		statements := make(map[string][]string)
		for _, action := range ident.Actions {
			// parse "Read:EXAMPLE-BUCKET"
			act := strings.Split(action, ":")

			resource := "*"
			if len(act) == 2 {
				resource = fmt.Sprintf("arn:aws:s3:::%s/*", act[1])
			}
			statements[resource] = append(statements[resource],
				fmt.Sprintf("s3:%s", MapToIdentitiesAction(act[0])),
			)
		}
		for resource, actions := range statements {
			isEqAction := false
			for i, statement := range policyDocument.Statement {
				if reflect.DeepEqual(statement.Action, actions) {
					policyDocument.Statement[i].Resource = append(
						policyDocument.Statement[i].Resource, resource)
					isEqAction = true
					break
				}
			}
			if isEqAction {
				continue
			}
			policyDocumentStatement := Statement{
				Effect: "Allow",
				Action: actions,
			}
			policyDocumentStatement.Resource = append(policyDocumentStatement.Resource, resource)
			policyDocument.Statement = append(policyDocument.Statement, &policyDocumentStatement)
		}
		resp.GetUserPolicyResult.PolicyDocument = policyDocument.String()
		return resp, nil
	}
	return resp, fmt.Errorf(iam.ErrCodeNoSuchEntityException)
}

func (iama *IamApiServer) DeleteUserPolicy(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp PutUserPolicyResponse, err error) {
	userName := values.Get("UserName")
	for i, ident := range s3cfg.Identities {
		if ident.Name == userName {
			s3cfg.Identities = append(s3cfg.Identities[:i], s3cfg.Identities[i+1:]...)
			return resp, nil
		}
	}
	return resp, fmt.Errorf(iam.ErrCodeNoSuchEntityException)
}

func GetActions(policy *PolicyDocument) (actions []string) {
	for _, statement := range policy.Statement {
		if statement.Effect != "Allow" {
			continue
		}
		for _, resource := range statement.Resource {
			// Parse "arn:aws:s3:::my-bucket/shared/*"
			res := strings.Split(resource, ":")
			if len(res) != 6 || res[0] != "arn" || res[1] != "aws" || res[2] != "s3" {
				glog.Infof("not match resource: %s", res)
				continue
			}
			for _, action := range statement.Action {
				// Parse "s3:Get*"
				act := strings.Split(action, ":")
				if len(act) != 2 || act[0] != "s3" {
					glog.Infof("not match action: %s", act)
					continue
				}
				statementAction := MapToStatementAction(act[1])
				if res[5] == "*" {
					actions = append(actions, statementAction)
					continue
				}
				// Parse my-bucket/shared/*
				path := strings.Split(res[5], "/")
				if len(path) != 2 || path[1] != "*" {
					glog.Infof("not match bucket: %s", path)
					continue
				}
				actions = append(actions, fmt.Sprintf("%s:%s", statementAction, path[0]))
			}
		}
	}
	return actions
}

func (iama *IamApiServer) CreateAccessKey(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp CreateAccessKeyResponse) {
	userName := values.Get("UserName")
	status := iam.StatusTypeActive
	accessKeyId := StringWithCharset(21, charsetUpper)
	secretAccessKey := StringWithCharset(42, charset)
	resp.CreateAccessKeyResult.AccessKey.AccessKeyId = &accessKeyId
	resp.CreateAccessKeyResult.AccessKey.SecretAccessKey = &secretAccessKey
	resp.CreateAccessKeyResult.AccessKey.UserName = &userName
	resp.CreateAccessKeyResult.AccessKey.Status = &status
	changed := false
	for _, ident := range s3cfg.Identities {
		if userName == ident.Name {
			ident.Credentials = append(ident.Credentials,
				&iam_pb.Credential{AccessKey: accessKeyId, SecretKey: secretAccessKey})
			changed = true
			break
		}
	}
	if !changed {
		s3cfg.Identities = append(s3cfg.Identities,
			&iam_pb.Identity{
				Name: userName,
				Credentials: []*iam_pb.Credential{
					{
						AccessKey: accessKeyId,
						SecretKey: secretAccessKey,
					},
				},
			},
		)
	}
	return resp
}

func (iama *IamApiServer) DeleteAccessKey(s3cfg *iam_pb.S3ApiConfiguration, values url.Values) (resp DeleteAccessKeyResponse) {
	userName := values.Get("UserName")
	accessKeyId := values.Get("AccessKeyId")
	for _, ident := range s3cfg.Identities {
		if userName == ident.Name {
			for i, cred := range ident.Credentials {
				if cred.AccessKey == accessKeyId {
					ident.Credentials = append(ident.Credentials[:i], ident.Credentials[i+1:]...)
					break
				}
			}
			break
		}
	}
	return resp
}

// handleImplicitUsername adds username who signs the request to values if 'username' is not specified
// According to https://awscli.amazonaws.com/v2/documentation/api/latest/reference/iam/create-access-key.html/
// "If you do not specify a user name, IAM determines the user name implicitly based on the Amazon Web
// Services access key ID signing the request."
func handleImplicitUsername(r *http.Request, values url.Values) {
	if len(r.Header["Authorization"]) == 0 || values.Get("UserName") != "" {
		return
	}
	// get username who signs the request. For a typical Authorization:
	// "AWS4-HMAC-SHA256 Credential=197FSAQ7HHTA48X64O3A/20220420/test1/iam/aws4_request, SignedHeaders=content-type;
	// host;x-amz-date, Signature=6757dc6b3d7534d67e17842760310e99ee695408497f6edc4fdb84770c252dc8",
	// the "test1" will be extracted as the username
	glog.V(4).Infof("Authorization field: %v", r.Header["Authorization"][0])
	s := strings.Split(r.Header["Authorization"][0], "Credential=")
	if len(s) < 2 {
		return
	}
	s = strings.Split(s[1], ",")
	if len(s) < 2 {
		return
	}
	s = strings.Split(s[0], "/")
	if len(s) < 5 {
		return
	}
	userName := s[2]
	values.Set("UserName", userName)
}

func (iama *IamApiServer) DoActions(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
		return
	}
	values := r.PostForm
	s3cfg := &iam_pb.S3ApiConfiguration{}
	if err := iama.s3ApiConfig.GetS3ApiConfiguration(s3cfg); err != nil {
		s3err.WriteErrorResponse(w, r, s3err.ErrInternalError)
		return
	}

	glog.V(4).Infof("DoActions: %+v", values)
	var response interface{}
	var err error
	changed := true
	switch r.Form.Get("Action") {
	case "ListUsers":
		response = iama.ListUsers(s3cfg, values)
		changed = false
	case "ListAccessKeys":
		handleImplicitUsername(r, values)
		response = iama.ListAccessKeys(s3cfg, values)
		changed = false
	case "CreateUser":
		response = iama.CreateUser(s3cfg, values)
	case "GetUser":
		userName := values.Get("UserName")
		response, err = iama.GetUser(s3cfg, userName)
		if err != nil {
			writeIamErrorResponse(w, r, err, "user", userName, nil)
			return
		}
		changed = false
	case "UpdateUser":
		response, err = iama.UpdateUser(s3cfg, values)
		if err != nil {
			glog.Errorf("UpdateUser: %+v", err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
			return
		}
	case "DeleteUser":
		userName := values.Get("UserName")
		response, err = iama.DeleteUser(s3cfg, userName)
		if err != nil {
			writeIamErrorResponse(w, r, err, "user", userName, nil)
			return
		}
	case "CreateAccessKey":
		handleImplicitUsername(r, values)
		response = iama.CreateAccessKey(s3cfg, values)
	case "DeleteAccessKey":
		handleImplicitUsername(r, values)
		response = iama.DeleteAccessKey(s3cfg, values)
	case "CreatePolicy":
		response, err = iama.CreatePolicy(s3cfg, values)
		if err != nil {
			glog.Errorf("CreatePolicy:  %+v", err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
			return
		}
	case "PutUserPolicy":
		response, err = iama.PutUserPolicy(s3cfg, values)
		if err != nil {
			glog.Errorf("PutUserPolicy:  %+v", err)
			s3err.WriteErrorResponse(w, r, s3err.ErrInvalidRequest)
			return
		}
	case "GetUserPolicy":
		response, err = iama.GetUserPolicy(s3cfg, values)
		if err != nil {
			writeIamErrorResponse(w, r, err, "user", values.Get("UserName"), nil)
			return
		}
		changed = false
	case "DeleteUserPolicy":
		if response, err = iama.DeleteUserPolicy(s3cfg, values); err != nil {
			writeIamErrorResponse(w, r, err, "user", values.Get("UserName"), nil)
			return
		}
	default:
		errNotImplemented := s3err.GetAPIError(s3err.ErrNotImplemented)
		errorResponse := ErrorResponse{}
		errorResponse.Error.Code = &errNotImplemented.Code
		errorResponse.Error.Message = &errNotImplemented.Description
		s3err.WriteXMLResponse(w, r, errNotImplemented.HTTPStatusCode, errorResponse)
		return
	}
	if changed {
		err := iama.s3ApiConfig.PutS3ApiConfiguration(s3cfg)
		if err != nil {
			writeIamErrorResponse(w, r, fmt.Errorf(iam.ErrCodeServiceFailureException), "", "", err)
			return
		}
	}
	s3err.WriteXMLResponse(w, r, http.StatusOK, response)
}
