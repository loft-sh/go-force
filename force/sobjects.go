package force

import (
	"bytes"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/biter777/countries"
	"github.com/nimajalali/go-force/sobjects"
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
			return nil, fmt.Errorf("Unable to find metadata for object: %v", in.ApiName())
		}

		uri := sObjectMetaData.URLs[sObjectDescribeKey]

		resp = &SObjectDescription{}
		err = forceApi.Get(uri, nil, resp)
		if err != nil {
			return nil, err
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

	return resp, nil
}

func (forceApi *ForceApi) GetSObject(id string, fields []string, out SObject) (err error) {
	uri := strings.Replace(forceApi.apiSObjects[out.ApiName()].URLs[rowTemplateKey], idKey, id, 1)

	params := url.Values{}
	if len(fields) > 0 {
		attributes, err := forceApi.GetAttributes(out, nil, false, true)
		if err != nil {
			return err
		}

		for i := range fields {
			attributes[fields[i]] = nil
		}

		// add base object fields
		fields = []string{}
		for k := range attributes {
			fields = append(fields, k)
		}

		params.Add("fields", strings.Join(fields, ","))
	}

	return forceApi.Get(uri, params, out.(interface{}))
}

func (forceApi *ForceApi) InsertSObject(in SObject, externalObj interface{}) (resp *SObjectResponse, err error) {
	uri := forceApi.apiSObjects[in.ApiName()].URLs[sObjectKey]
	resp = &SObjectResponse{}

	attributes, err := forceApi.GetAttributes(in, externalObj, true, false)
	if err != nil {
		return nil, err
	}
	err = forceApi.Post(uri, nil, attributes, resp)
	if err != nil {
		return nil, err
	}

	return resp, err
}

func (forceApi *ForceApi) UpdateSObject(id string, in SObject, externalObj interface{}) (err error) {
	uri := strings.Replace(forceApi.apiSObjects[in.ApiName()].URLs[rowTemplateKey], idKey, id, 1)

	attributes, err := forceApi.GetAttributes(in, externalObj, false, false)
	if err != nil {
		return err
	}

	return forceApi.Patch(uri, nil, attributes, nil)
}

func (forceApi *ForceApi) Debug(enable bool) {
	forceApi.debugMode = enable
}

type attribute struct {
	Value                  interface{}
	IsValueFromExternalObj bool
}

func (forceApi *ForceApi) GetAttributes(in SObject, externalObj interface{}, isInsert bool, isGet bool) (map[string]interface{}, error) {
	fieldsByTag := map[string]attribute{}

	ref := reflect.ValueOf(in)
	if ref.Kind() == reflect.Pointer {
		ref = ref.Elem()
	}

	externalObjRef := reflect.ValueOf(externalObj)

	rt := ref.Type()
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		// use split to ignore tag "options"
		fieldNameSFDC := strings.Split(field.Tag.Get("force"), ",")[0]
		if fieldNameSFDC == "" || fieldNameSFDC == "-" {
			continue
		}

		// use split to ignore tag "options"
		fieldNameExternal := strings.Split(field.Tag.Get("ext"), ",")[0]
		val, isValueFromExternalObj := getFieldValue(ref, field, externalObjRef, fieldNameExternal)

		isCountryField := strings.HasSuffix(fieldNameSFDC, "Country")
		if isCountryField || strings.HasSuffix(fieldNameSFDC, "State") {
			stringVal, ok := val.(string)
			if ok {
				if len(stringVal) != 2 {
					country := countries.ByName(stringVal)
					countryCode := country.Alpha2()
					if len(countryCode) == 2 {
						stringVal = countryCode
					}
				}

				if len(stringVal) == 2 {
					codeFieldName := fieldNameSFDC + "Code"
					_, codeFieldExists := rt.FieldByName(codeFieldName)
					if codeFieldExists {
						fieldNameSFDC = codeFieldName
						val = stringVal
					}
				}
			}
		}

		fieldsByTag[fieldNameSFDC] = attribute{
			Value:                  val,
			IsValueFromExternalObj: isValueFromExternalObj,
		}
	}

	objectDescription, err := forceApi.DescribeSObject(in)
	if err != nil {
		return nil, err
	}

	attributes := map[string]interface{}{}
	for _, field := range objectDescription.Fields {
		fieldName := field.Name
		isRelationship := field.RelationshipName != ""
		if isRelationship {
			fieldName = field.RelationshipName
		}

		attribute, ok := fieldsByTag[fieldName]
		if ok {
			val := attribute.Value
			if isGet {
				if isRelationship {
					attributes[fieldName+".Id"] = val
				} else {
					attributes[field.Name] = val
				}
			} else if val != nil {
				if field.Type == "currency" && attribute.IsValueFromExternalObj {
					valFloat64, ok := val.(float64)
					if ok {
						val = valFloat64 / 100
					}
				} else if field.Name == "CurrencyIsoCode" {
					val = strings.ToUpper(val.(string))
				} else if isRelationship {
					valRef := reflect.ValueOf(val)
					if valRef.Kind() == reflect.Struct {
						idField, ok := valRef.Type().FieldByName("Id")
						if ok {
							val, _ = getFieldValue(valRef, idField, reflect.Value{}, "")
						}
					}
					attributes[field.Name] = val
				}

				if field.Updateable {
					attributes[field.Name] = val
				}
			}
		}
	}

	return attributes, nil
}

var sobjectType = reflect.TypeOf((*SObject)(nil)).Elem()

func getFieldValue(ref reflect.Value, field reflect.StructField, externalObjRef reflect.Value, fieldNameExternal string) (interface{}, bool) {
	isValueFromExternalObj := false
	if externalObjRef.Kind() == reflect.Pointer {
		externalObjRef = externalObjRef.Elem()
	}

	fieldValue := ref.FieldByName(field.Name)
	switch externalObjRef.Kind() {
	case reflect.Struct:
		if fieldNameExternal != "" && fieldNameExternal != "-" {
			fieldNameExternalSplit := strings.Split(fieldNameExternal, ".")
			fieldValueExternal := externalObjRef.FieldByName(fieldNameExternalSplit[0])
			if fieldValueExternal.IsValid() && !fieldValueExternal.IsZero() {
				if len(fieldNameExternalSplit) > 1 {
					return getFieldValue(ref, field, fieldValueExternal, strings.Join(fieldNameExternalSplit[1:], "."))
				}

				isValueFromExternalObj = true

				refType := fieldValue.Type()
				if refType.Kind() == reflect.Pointer {
					refType = refType.Elem()
				}

				if refType.Kind() == reflect.Struct {
					switch refType {
					case reflect.TypeOf(sobjects.Time{}):
						t := time.Unix(fieldValueExternal.Int(), 0)
						fieldValueExternal = reflect.ValueOf(sobjects.AsTime(t))
						fieldValue.Set(fieldValueExternal)
						setValue(fieldValue, fieldValueExternal)
					default:
						setValue(fieldValue, fieldValueExternal)
					}
				} else {
					if refType.Kind() == reflect.Float64 && fieldValueExternal.Kind() == reflect.Int64 {
						fieldValueExternal = reflect.ValueOf(float64(fieldValueExternal.Int()))
					}
					setValue(fieldValue, fieldValueExternal)
				}
			}
		}
	case reflect.Map:
		mapValue := externalObjRef.MapIndex(reflect.ValueOf(fieldNameExternal))
		if mapValue.IsValid() && !mapValue.IsZero() {
			isValueFromExternalObj = true
			fieldValue = mapValue
		}
	}

	if fieldValue.Kind() == reflect.Pointer {
		fieldValue = fieldValue.Elem()
	}

	switch fieldValue.Kind() {
	case reflect.String:
		return fieldValue.String(), isValueFromExternalObj
	case reflect.Bool:
		return fieldValue.Bool(), isValueFromExternalObj
	case reflect.Int64:
		return fieldValue.Int(), isValueFromExternalObj
	case reflect.Float64:
		return fieldValue.Float(), isValueFromExternalObj
	case reflect.Struct:
		val := fieldValue.Interface()
		if fieldValue.Type().Implements(sobjectType) {
			idField := fieldValue.FieldByName("Id")
			if idField.IsValid() {
				val = idField.Interface()
			}
		}
		return val, isValueFromExternalObj
	}
	return nil, isValueFromExternalObj
}

func setValue(fieldValue reflect.Value, fieldValueExternal reflect.Value) {
	if fieldValue.Kind() == reflect.Pointer && fieldValueExternal.Kind() != reflect.Pointer {
		fieldValueExternalPtr := reflect.New(fieldValue.Type().Elem())
		setValue(fieldValueExternalPtr.Elem(), fieldValueExternal)
		fieldValueExternal = fieldValueExternalPtr
	}

	switch fieldValue.Kind() {
	case reflect.String:
		fieldValue.SetString(fieldValueExternal.String())
	case reflect.Bool:
		fieldValue.SetBool(fieldValueExternal.Bool())
	case reflect.Int64:
		fieldValue.SetInt(fieldValueExternal.Int())
	case reflect.Float64:
		fieldValue.SetFloat(fieldValueExternal.Float())
	default:
		fieldValue.Set(fieldValueExternal)
	}
}

func (forceApi *ForceApi) DeleteSObject(id string, in SObject) (err error) {
	uri := strings.Replace(forceApi.apiSObjects[in.ApiName()].URLs[rowTemplateKey], idKey, id, 1)

	return forceApi.Delete(uri, nil)
}

func (forceApi *ForceApi) GetSObjectByExternalId(id string, fields []string, out SObject) (err error) {
	uri := fmt.Sprintf("%v/%v/%v", forceApi.apiSObjects[out.ApiName()].URLs[sObjectKey],
		out.ExternalIdApiName(), id)

	params := url.Values{}
	if len(fields) > 0 {
		params.Add("fields", strings.Join(fields, ","))
	}

	return forceApi.Get(uri, params, out.(interface{}))
}

func (forceApi *ForceApi) UpsertSObjectByExternalId(id string, in SObject, externalObj interface{}) (resp *SObjectResponse, err error) {
	uri := fmt.Sprintf("%v/%v/%v", forceApi.apiSObjects[in.ApiName()].URLs[sObjectKey],
		in.ExternalIdApiName(), id)

	resp = &SObjectResponse{}

	attributes, err := forceApi.GetAttributes(in, externalObj, false, false)
	if err != nil {
		return nil, err
	}

	delete(attributes, in.ExternalIdApiName())

	err = forceApi.Patch(uri, nil, attributes, resp)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (forceApi *ForceApi) DeleteSObjectByExternalId(id string, in SObject) (err error) {
	uri := fmt.Sprintf("%v/%v/%v", forceApi.apiSObjects[in.ApiName()].URLs[sObjectKey],
		in.ExternalIdApiName(), id)

	return forceApi.Delete(uri, nil)
}
