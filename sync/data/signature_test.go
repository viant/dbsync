package data

import (
	"dbsync/sync/sql/diff"
	"fmt"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestSignature_ValidateIDConsistency(t *testing.T) {

	var useCases = []struct {
		description string
		id          string
		record      Record
		error       string
	}{
		{
			description: "record ID consistent",
			id:          "id",
			record: Record{
				"cnt": 10,
				fmt.Sprintf(diff.AliasUniqueIDCountTemplate, "id"):  10,
				fmt.Sprintf(diff.AliasNonNullIDCountTemplate, "id"): 10,
			},
		},


		{
			description: "record consistent - no id",
			id:          "",
			record: Record{
				"cnt": 10,
				fmt.Sprintf(diff.AliasUniqueIDCountTemplate, "id"):  10,
				fmt.Sprintf(diff.AliasNonNullIDCountTemplate, "id"): 10,
			},
		},

		{
			description: "record consistent - no data",
			id:          "id",
			record: Record{},
		},

		{
			description: "record inconsistent - partial data",
			id:          "id",
			record: Record{
				"cnt": 10,
				fmt.Sprintf(diff.AliasUniqueIDCountTemplate, "id"):    8,
			},
			error: "unique column has NULL values",
		},


		{
			description: "record ID inconsistent -  null IDs valuues",
			id:          "id",
			record: Record{
				"cnt": 20,
				fmt.Sprintf(diff.AliasUniqueIDCountTemplate, "id"):  10,
				fmt.Sprintf(diff.AliasNonNullIDCountTemplate, "id"): 10,
			},
			error: "unique column has NULL values",
		},
		{
			description: "record ID inconsistent - ID duplicates",
			id:          "id",
			record: Record{
				"cnt": 20,
				fmt.Sprintf(diff.AliasUniqueIDCountTemplate, "id"):  15,
				fmt.Sprintf(diff.AliasNonNullIDCountTemplate, "id"): 20,
			},
			error: "data has unique ID duplicates",
		},
	}

	for _, useCase := range useCases {

		signature := NewSignatureFromRecord(useCase.id, useCase.record)
		actual := signature.ValidateIDConsistency()
		if useCase.error == "" {
			assert.Nil(t, actual, useCase.description)
			continue
		}
		if assert.NotNil(t, actual, useCase.description) {
			assert.True(t, strings.Contains(actual.Error(), useCase.error), useCase.description+" "+actual.Error())
		}
	}

}

func TestSignature_IsEqual(t *testing.T) {

	var useCases = []struct {
		description string
		id          string
		record1      Record
		record2      Record
		expect      bool
	}{

		{
			description:"equal",
			id:"id",

			record1: Record{
				"cnt": 10,
				fmt.Sprintf(diff.AliasMaxIdTemplate, "id"):    10,
				fmt.Sprintf(diff.AliasMinIdTemplate, "id"): 1,
			},
			record2: Record{
				"cnt": 10,
				fmt.Sprintf(diff.AliasMaxIdTemplate, "id"):    10,
				fmt.Sprintf(diff.AliasMinIdTemplate, "id"): 1,
			},
			expect:true,

		},

		{
			description:"equal - no data",
			id:"id",

			record1: Record{},
			record2: Record{},
			expect:true,

		},
		{
			description:"not equal count diff",
			id:"id",

			record1: Record{
				"cnt": 10,
				fmt.Sprintf(diff.AliasMaxIdTemplate, "id"):    10,
				fmt.Sprintf(diff.AliasMinIdTemplate, "id"): 1,
			},
			record2: Record{
				"cnt": 15,
				fmt.Sprintf(diff.AliasMaxIdTemplate, "id"):    10,
				fmt.Sprintf(diff.AliasMinIdTemplate, "id"): 1,
			},
			expect:false,

		},

		{
			description:"not equal min diff",
			id:"id",

			record1: Record{
				"cnt": 10,
				fmt.Sprintf(diff.AliasMaxIdTemplate, "id"):    10,
				fmt.Sprintf(diff.AliasMinIdTemplate, "id"): 2,
			},
			record2: Record{
				"cnt": 10,
				fmt.Sprintf(diff.AliasMaxIdTemplate, "id"):    10,
				fmt.Sprintf(diff.AliasMinIdTemplate, "id"): 1,
			},
			expect:false,

		},


		{
			description:"not equal max diff",
			id:"id",

			record1: Record{
				"cnt": 10,
				fmt.Sprintf(diff.AliasMaxIdTemplate, "id"):    10,
				fmt.Sprintf(diff.AliasMinIdTemplate, "id"): 1,
			},
			record2: Record{
				"cnt": 10,
				fmt.Sprintf(diff.AliasMaxIdTemplate, "id"):    15,
				fmt.Sprintf(diff.AliasMinIdTemplate, "id"): 1,
			},
			expect:false,

		},

	}

	for _, useCase := range useCases {
		signature1 := NewSignatureFromRecord(useCase.id, useCase.record1)
		signature2 := NewSignatureFromRecord(useCase.id, useCase.record2)

		actual := signature1.IsEqual(signature2)
		assert.EqualValues(t, useCase.expect, actual, useCase.description)
	}

}
