package force

import (
	"bytes"
	"fmt"
	"net/url"
	"reflect"
	"strings"
)

// Interface all standard and custom objects must implement. Needed for uri generation.
type SObject interface {
	ApiName() string
	ExternalIdApiName() string
}

// Response received from force.com API after insert of an sobject.
type SObjectResponse struct {
	Id      string    `force:"id,omitempty"`
	Errors  ApiErrors `force:"error,omitempty"` //TODO: Not sure if ApiErrors is the right object
	Success bool      `force:"success,omitempty"`
}

func (forceAPI *ForceApi) DescribeSObjects() (map[string]*SObjectMetaData, error) {
	if err := forceAPI.getApiSObjects(); err != nil {
		return nil, err
	}

	return forceAPI.apiSObjects, nil
}

func (forceApi *ForceApi) DescribeSObject(in SObject) (resp *SObjectDescription, err error) {
	// Check cache
	resp, ok := forceApi.apiSObjectDescriptions[in.ApiName()]
	if !ok {
		// Attempt retrieval from api
		sObjectMetaData, ok := forceApi.apiSObjects[in.ApiName()]
		if !ok {
			err = fmt.Errorf("Unable to find metadata for object: %v", in.ApiName())
			return
		}

		uri := sObjectMetaData.URLs[sObjectDescribeKey]

		resp = &SObjectDescription{}
		err = forceApi.Get(uri, nil, resp)
		if err != nil {
			return
		}

		// Create Comma Separated String of All Field Names.
		// Used for SELECT * Queries.
		length := len(resp.Fields)
		if length > 0 {
			var allFields bytes.Buffer
			for index, field := range resp.Fields {
				// Field type location cannot be directly retrieved from SQL Query.
				if field.Type != "location" {
					if index > 0 && index < length {
						allFields.WriteString(", ")
					}
					allFields.WriteString(field.Name)
				}
			}

			resp.AllFields = allFields.String()
		}

		forceApi.apiSObjectDescriptions[in.ApiName()] = resp
	}

	return
}

func (forceApi *ForceApi) GetSObject(id string, fields []string, out SObject) (err error) {
	uri := strings.Replace(forceApi.apiSObjects[out.ApiName()].URLs[rowTemplateKey], idKey, id, 1)

	params := url.Values{}
	if len(fields) > 0 {
		params.Add("fields", strings.Join(fields, ","))
	}

	err = forceApi.Get(uri, params, out.(interface{}))

	return
}

func (forceApi *ForceApi) InsertSObject(in SObject) (resp *SObjectResponse, err error) {
	uri := forceApi.apiSObjects[in.ApiName()].URLs[sObjectKey]
	resp = &SObjectResponse{}

	attributes, err := forceApi.getAttributes(in, true)
	if err != nil {
		return nil, err
	}
	err = forceApi.Post(uri, nil, attributes, resp)

	return
}

func (forceApi *ForceApi) UpdateSObject(id string, in SObject) (err error) {
	uri := strings.Replace(forceApi.apiSObjects[in.ApiName()].URLs[rowTemplateKey], idKey, id, 1)

	attributes, err := forceApi.getAttributes(in, false)
	if err != nil {
		return err
	}
	err = forceApi.Patch(uri, nil, attributes, nil)

	return
}

func (forceApi *ForceApi) getAttributes(in SObject, isInsert bool) (map[string]interface{}, error) {
	fieldsByTag := map[string]interface{}{}
	key := "force"

	ref := reflect.ValueOf(in)

	if ref.Kind() == reflect.Pointer {
		ref = ref.Elem()
	}

	rt := ref.Type()
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		fieldName := strings.Split(f.Tag.Get(key), ",")[0] // use split to ignore tag "options"
		if fieldName == "" || fieldName == "-" {
			continue
		}

		fieldValue := ref.FieldByName(f.Name)
		if fieldValue.Kind() == reflect.Pointer {
			fieldValue = fieldValue.Elem()
		}

		var val interface{}

		switch fieldValue.Kind() {
		case reflect.String:
			val = fieldValue.String()
		case reflect.Bool:
			val = fieldValue.Bool()
		case reflect.Int64:
			val = fieldValue.Int()
		case reflect.Struct:
			val = fieldValue.Interface()
		}
		fieldsByTag[fieldName] = val
	}

	objectDescription, err := forceApi.DescribeSObject(in)
	if err != nil {
		return nil, err
	}

	attributes := map[string]interface{}{}
	for _, field := range objectDescription.Fields {
		if field.Updateable {
			val, ok := fieldsByTag[field.Name]
			if ok && val != nil {
				attributes[field.Name] = val
			}
		}
	}

	return attributes, nil
}

func (forceApi *ForceApi) DeleteSObject(id string, in SObject) (err error) {
	uri := strings.Replace(forceApi.apiSObjects[in.ApiName()].URLs[rowTemplateKey], idKey, id, 1)

	err = forceApi.Delete(uri, nil)

	return
}

func (forceApi *ForceApi) GetSObjectByExternalId(id string, fields []string, out SObject) (err error) {
	uri := fmt.Sprintf("%v/%v/%v", forceApi.apiSObjects[out.ApiName()].URLs[sObjectKey],
		out.ExternalIdApiName(), id)

	params := url.Values{}
	if len(fields) > 0 {
		params.Add("fields", strings.Join(fields, ","))
	}

	err = forceApi.Get(uri, params, out.(interface{}))

	return
}

func (forceApi *ForceApi) UpsertSObjectByExternalId(id string, in SObject) (resp *SObjectResponse, err error) {
	uri := fmt.Sprintf("%v/%v/%v", forceApi.apiSObjects[in.ApiName()].URLs[sObjectKey],
		in.ExternalIdApiName(), id)

	resp = &SObjectResponse{}

	attributes, err := forceApi.getAttributes(in, false)
	if err != nil {
		return nil, err
	}

	delete(attributes, in.ExternalIdApiName())

	err = forceApi.Patch(uri, nil, attributes, resp)

	return
}

func (forceApi *ForceApi) DeleteSObjectByExternalId(id string, in SObject) (err error) {
	uri := fmt.Sprintf("%v/%v/%v", forceApi.apiSObjects[in.ApiName()].URLs[sObjectKey],
		in.ExternalIdApiName(), id)

	err = forceApi.Delete(uri, nil)

	return
}
