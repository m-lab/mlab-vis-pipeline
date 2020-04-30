package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/m-lab/mlab-vis-pipeline/incidents/incident"
	"google.golang.org/api/option"
)

// Test takes around 5 minutes to run
// Check that the CSV was generated
func Test_findIncidents(t *testing.T) {

	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "Test that CSV is generated",
			input: INCIDENT_CSV,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findIncidents()
			// Attempt to open the file
			_, err := os.Open(tt.input)

			if err != nil {
				t.Errorf("Cannot open '%s':%s\n", tt.input, err.Error())
			} else {
				// File generation was confirmed, remove to clean test state
				os.Remove(INCIDENT_CSV)
			}
		})
	}
}

// Test takes more than 10 minutes to run with runPipeline() uncommented
// Checks if there are particular incidents in each of the top level directories in
// the incidents-location-hierarchy bucket
// Leaving the call to runPipeline() commented out assumes that these incidents
// already exist in the bucket on Google Cloud Storage
func Test_runPipeline(t *testing.T) {
	// Uncomment this call to update incidents on Google Cloud Storage
	// runPipeline()

	afTest := incident.IncidentJsonData{
		GoodPeriodStart:  time.Date(2010, time.September, 1, 0, 0, 0, 0, time.UTC),
		GoodPeriodEnd:    time.Date(2011, time.September, 1, 0, 0, 0, 0, time.UTC),
		BadPeriodStart:   time.Date(2011, time.September, 1, 0, 0, 0, 0, time.UTC),
		BadPeriodEnd:     time.Date(2013, time.November, 1, 0, 0, 0, 0, time.UTC),
		GoodPeriodMetric: 0.319012,
		BadPeriodMetric:  0.114054,
		ASN:              "AS15399",
		Location:         "afke",
		Severity:         0.639755,
		NumTestsAffected: 1470,
		GoodPeriodInfo:   "Average download speed: 0.32 mb/s",
		BadPeriodInfo:    "Average download speed: 0.11 mb/s",
		IncidentInfo:     "Download speed dropped by 63.98% affecting 1470 tests",
	}
	asTest := incident.IncidentJsonData{
		GoodPeriodStart:  time.Date(2013, time.May, 1, 0, 0, 0, 0, time.UTC),
		GoodPeriodEnd:    time.Date(2014, time.May, 1, 0, 0, 0, 0, time.UTC),
		BadPeriodStart:   time.Date(2014, time.May, 1, 0, 0, 0, 0, time.UTC),
		BadPeriodEnd:     time.Date(2016, time.December, 1, 0, 0, 0, 0, time.UTC),
		GoodPeriodMetric: 14.430182,
		BadPeriodMetric:  9.560504,
		ASN:              "AS2497",
		Location:         "asjp",
		Severity:         0.509531,
		NumTestsAffected: 1429,
		GoodPeriodInfo:   "Average download speed: 14.43 mb/s",
		BadPeriodInfo:    "Average download speed: 9.56 mb/s",
		IncidentInfo:     "Download speed dropped by 50.95% affecting 1429 tests",
	}
	euTest := incident.IncidentJsonData{
		GoodPeriodStart:  time.Date(2013, time.March, 1, 0, 0, 0, 0, time.UTC),
		GoodPeriodEnd:    time.Date(2014, time.March, 1, 0, 0, 0, 0, time.UTC),
		BadPeriodStart:   time.Date(2014, time.March, 1, 0, 0, 0, 0, time.UTC),
		BadPeriodEnd:     time.Date(2017, time.September, 1, 0, 0, 0, 0, time.UTC),
		GoodPeriodMetric: 24.017117,
		BadPeriodMetric:  15.586552,
		ASN:              "AS1136",
		Location:         "eu",
		Severity:         0.461079,
		NumTestsAffected: 13685,
		GoodPeriodInfo:   "Average download speed: 24.02 mb/s",
		BadPeriodInfo:    "Average download speed: 15.59 mb/s",
		IncidentInfo:     "Download speed dropped by 46.11% affecting 13685 tests",
	}
	naTest := incident.IncidentJsonData{
		GoodPeriodStart:  time.Date(2015, time.July, 1, 0, 0, 0, 0, time.UTC),
		GoodPeriodEnd:    time.Date(2016, time.July, 1, 0, 0, 0, 0, time.UTC),
		BadPeriodStart:   time.Date(2016, time.July, 1, 0, 0, 0, 0, time.UTC),
		BadPeriodEnd:     time.Date(2018, time.April, 1, 0, 0, 0, 0, time.UTC),
		GoodPeriodMetric: 7.862733,
		BadPeriodMetric:  5.334354,
		ASN:              "AS10774x",
		Location:         "nauscalosangeles",
		Severity:         0.321565,
		NumTestsAffected: 68089,
		GoodPeriodInfo:   "Average download speed: 7.86 mb/s",
		BadPeriodInfo:    "Average download speed: 5.33 mb/s",
		IncidentInfo:     "Download speed dropped by 32.16% affecting 68089 tests",
	}
	ocTest := incident.IncidentJsonData{
		GoodPeriodStart:  time.Date(2013, time.May, 1, 0, 0, 0, 0, time.UTC),
		GoodPeriodEnd:    time.Date(2014, time.May, 1, 0, 0, 0, 0, time.UTC),
		BadPeriodStart:   time.Date(2014, time.May, 1, 0, 0, 0, 0, time.UTC),
		BadPeriodEnd:     time.Date(2017, time.August, 1, 0, 0, 0, 0, time.UTC),
		GoodPeriodMetric: 6.335732,
		BadPeriodMetric:  4.615034,
		ASN:              "AS10143",
		Location:         "ocau",
		Severity:         0.546351,
		NumTestsAffected: 30120,
		GoodPeriodInfo:   "Average download speed: 6.34 mb/s",
		BadPeriodInfo:    "Average download speed: 4.62 mb/s",
		IncidentInfo:     "Download speed dropped by 54.64% affecting 30120 tests",
	}
	saTest := incident.IncidentJsonData{
		GoodPeriodStart:  time.Date(2012, time.February, 1, 0, 0, 0, 0, time.UTC),
		GoodPeriodEnd:    time.Date(2013, time.February, 1, 0, 0, 0, 0, time.UTC),
		BadPeriodStart:   time.Date(2013, time.February, 1, 0, 0, 0, 0, time.UTC),
		BadPeriodEnd:     time.Date(2015, time.June, 1, 0, 0, 0, 0, time.UTC),
		GoodPeriodMetric: 0.864799,
		BadPeriodMetric:  0.580199,
		ASN:              "AS22927",
		Location:         "sa",
		Severity:         0.348264,
		NumTestsAffected: 15180,
		GoodPeriodInfo:   "Average download speed: 0.86 mb/s",
		BadPeriodInfo:    "Average download speed: 0.58 mb/s",
		IncidentInfo:     "Download speed dropped by 34.83% affecting 15180 tests",
	}

	// Creating a Google Cloud Storage client
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		t.Errorf("Could not create GCS client")
	}
	// Obtain a bucket handle
	bkt := client.Bucket(BUCKET_NAME)

	tests := []struct {
		name  string
		input string
		want  incident.IncidentJsonData
	}{
		{
			name:  "Confirm incident in 'af' directory",
			input: "af/ke/AS15399.json",
			want:  afTest,
		},
		{
			name:  "Confirm incident in 'as' directory",
			input: "as/jp/AS2497.json",
			want:  asTest,
		},
		{
			name:  "Confirm incident in 'eu' directory",
			input: "eu/AS1136.json",
			want:  euTest,
		},
		{
			name:  "Confirm incident in 'na' directory",
			input: "na/us/ca/losangeles/AS10774x.json",
			want:  naTest,
		},
		{
			name:  "Confirm incident in 'oc' directory",
			input: "oc/au/AS10143.json",
			want:  ocTest,
		},
		{
			name:  "Confirm incident in 'sa' directory",
			input: "sa/AS22927.json",
			want:  saTest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// Obtain an object handle for a particular incident in the tested region
			obj := bkt.Object(tt.input)
			r, err := obj.NewReader(ctx)
			if err != nil {
				t.Errorf("Could not create object reader")
			}
			defer r.Close()
			data, err := ioutil.ReadAll(r)
			if err != nil {
				t.Errorf("Could not create read object")
			}
			var resultIncidentJson []incident.IncidentJsonData
			json.Unmarshal(data, &resultIncidentJson)
			if resultIncidentJson[0] != tt.want {
				t.Errorf("JSON objects are not the same")
				fmt.Print(resultIncidentJson)
			}
		})
	}
}
