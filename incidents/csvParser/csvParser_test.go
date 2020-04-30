package csvParser

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/m-lab/clinic2019/incident_viewer_demo/incident"
)

func generateTestIncidents() []incident.DefaultIncident {

	testIncident := new(incident.DefaultIncident)
	testIncident.MakeIncidentData(
		time.Date(2016, time.July, 1, 0, 0, 0, 0, time.UTC).AddDate(-1, 0, 0),
		time.Date(2016, time.July, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2016, time.July, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2018, time.April, 1, 0, 0, 0, 0, time.UTC),
		7.862733,
		5.334354,
		"AS10774x",
		"eufr",
		0.3565,
		68089,
	)

	testIncidentTwo := new(incident.DefaultIncident)
	testIncidentTwo.MakeIncidentData(
		time.Date(2016, time.July, 1, 0, 0, 0, 0, time.UTC).AddDate(-1, 0, 0),
		time.Date(2016, time.July, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2016, time.July, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2018, time.April, 1, 0, 0, 0, 0, time.UTC),
		7.862733,
		5.334354,
		"AS10774x",
		"naus",
		0.3565,
		68089,
	)

	testIncidentThree := new(incident.DefaultIncident)
	testIncidentThree.MakeIncidentData(
		time.Date(2016, time.July, 1, 0, 0, 0, 0, time.UTC).AddDate(-1, 0, 0),
		time.Date(2016, time.July, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2016, time.July, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2018, time.April, 1, 0, 0, 0, 0, time.UTC),
		7.862733,
		5.334354,
		"AS10774x",
		"eufr",
		0.3575,
		69089,
	)

	testIncidentFour := new(incident.DefaultIncident)
	testIncidentFour.MakeIncidentData(
		time.Date(2016, time.July, 1, 0, 0, 0, 0, time.UTC).AddDate(-1, 0, 0),
		time.Date(2016, time.July, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2016, time.July, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2018, time.April, 1, 0, 0, 0, 0, time.UTC),
		7.862733,
		5.334354,
		"AS10774z",
		"eufr",
		0.3565,
		68089,
	)

	testIncidentsArr := make([]incident.DefaultIncident, 0)
	testIncidentsArr = append(testIncidentsArr, *testIncident)
	testIncidentsArr = append(testIncidentsArr, *testIncidentTwo)
	testIncidentsArr = append(testIncidentsArr, *testIncidentThree)
	testIncidentsArr = append(testIncidentsArr, *testIncidentFour)

	return testIncidentsArr
}

func Test_CsvParserEntries(t *testing.T) {

	// Create test incident to check result against
	var testIncident incident.DefaultIncident = incident.DefaultIncident{}

	var gs = time.Date(2016, time.January, 1, 0, 0, 0, 0, time.UTC)
	var ge = time.Date(2017, time.January, 1, 0, 0, 0, 0, time.UTC)
	var bs = time.Date(2017, time.January, 1, 0, 0, 0, 0, time.UTC)
	var be = time.Date(2019, time.April, 1, 0, 0, 0, 0, time.UTC)
	testIncident.MakeIncidentData(gs, ge, bs, be, 31.046068, 21.27035, "AS11486x", "naus", 0.326146, 4788063)

	tests := []struct {
		name string

		input string
		want  incident.DefaultIncident
	}{
		{
			name:  "The first entry in the incidents array",
			input: "incidentfile.csv",
			want:  testIncident,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			incidents := CsvParser(tt.input, 1)
			got := incidents[0]
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CsvParser() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_numIncidentsSize(t *testing.T) {

	tests := []struct {
		name  string
		input string
		want  int
	}{
		{
			name:  "The size of the incidents array as specified by numIncidents",
			input: "incidentfile.csv",
			want:  40,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			incidents := CsvParser(tt.input, tt.want)
			got := len(incidents)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CsvParser() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_CsvParserFiftyFirstEntry(t *testing.T) {

	// Create test incident to check result against
	var testIncident incident.DefaultIncident = incident.DefaultIncident{}

	var gs = time.Date(2015, time.July, 1, 0, 0, 0, 0, time.UTC)
	var ge = time.Date(2016, time.July, 1, 0, 0, 0, 0, time.UTC)
	var bs = time.Date(2016, time.July, 1, 0, 0, 0, 0, time.UTC)
	var be = time.Date(2018, time.April, 1, 0, 0, 0, 0, time.UTC)
	testIncident.MakeIncidentData(gs, ge, bs, be, 7.862733, 5.334354, "AS10774x", "nauscalosangeles", 0.321565, 68089)

	tests := []struct {
		name  string
		input string
		want  incident.DefaultIncident
	}{
		{
			name:  "The 51st entry in the incident array",
			input: "incidentfile.csv",
			want:  testIncident,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// generate 51 incidents
			incidents := CsvParser(tt.input)
			got := incidents[50]
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CsvParser() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_FileHierachy(t *testing.T) {
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	// Same directory as the executable
	rootPath := filepath.Dir(ex)

	incidentMap := mapIncidentsToLocAndISP(generateTestIncidents())

	locationCodesArr := make([]string, 0)
	locationCodesArr = append(locationCodesArr, "/eu/fr")
	locationCodesArr = append(locationCodesArr, "/eu/fr/AS10774x.json")
	locationCodesArr = append(locationCodesArr, "/eu/fr/AS10774z.json")
	locationCodesArr = append(locationCodesArr, "/na/us")

	tests := []struct {
		name  string
		input []string
	}{
		{
			name:  "Check if the directories are being created",
			input: locationCodesArr,
		},
	}

	placeIncidentsInFileHierarchy(rootPath, incidentMap)

	for _, tt := range tests {
		i := 0
		t.Run(tt.name, func(t *testing.T) {
			for i != 4 {
				if _, err := os.Stat(rootPath + tt.input[i]); os.IsNotExist(err) {
					t.Errorf("File does not exist")
				}
				i++
			}
		})

		// Set things up to run file hierarchy again to simulate two consecutive monthly run jobs
		testIncident := new(incident.DefaultIncident)
		testIncident.MakeIncidentData(time.Date(2017, time.July, 1, 0, 0, 0, 0, time.UTC).AddDate(-1, 0, 0),
			time.Date(2017, time.July, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2017, time.July, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2019, time.April, 1, 0, 0, 0, 0, time.UTC),
			7.862733,
			5.334354,
			"AS10774x",
			"eufr",
			0.3565, 68089)

		theNextMonthIncidents := append(generateTestIncidents(), *testIncident)
		theNextMonthIcidentMap := mapIncidentsToLocAndISP(theNextMonthIncidents)

		placeIncidentsInFileHierarchy(rootPath, theNextMonthIcidentMap)

		jsonFile, _ := os.Open(rootPath + "/eu/fr/AS10774x.json")
		byteValue, _ := ioutil.ReadAll(jsonFile)
		var incidents []incident.IncidentJsonData = make([]incident.IncidentJsonData, 0)
		json.Unmarshal(byteValue, &incidents)

		t.Run(tt.name, func(t *testing.T) {
			for i != 4 {
				if _, err := os.Stat(rootPath + tt.input[i]); os.IsNotExist(err) {
					t.Errorf("File does not exist")
				}
				i++
			}
			if len(incidents) != 3 {
				t.Errorf("Rerun is not working properly")
			}
		})
	}
}

func Test_ParseLocationCode(t *testing.T) {
	locationCodesArr := make([]string, 0)
	locationCodesArr = append(locationCodesArr, "na")
	locationCodesArr = append(locationCodesArr, "us")
	locationCodesArr = append(locationCodesArr, "wa")
	locationCodesArr = append(locationCodesArr, "redmond")

	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "Parsing the location code string",
			input: "nauswaredmond",
			want:  locationCodesArr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsedCode := parseLocationCode(tt.input)
			if !reflect.DeepEqual(parsedCode, tt.want) {
				t.Errorf("parseLocationCode() = %v, want %v", parsedCode, tt.want)
			}
		})
	}

}

func Test_incidentsMemPlacer(t *testing.T) {

	tests := []struct {
		name     string
		input    string
		inputTwo string
		want     string
	}{
		{
			name:     "A location that should exist in the map",
			input:    "AS10774x",
			inputTwo: "naus",
			want:     "naus",
		},

		{
			name:     "A location that has two ISPs associated w/ it",
			input:    "AS10774z",
			inputTwo: "eufr",
			want:     "eufr",
		},

		{
			name:     "A location that has two ISPs associated w/ it",
			input:    "AS10774x",
			inputTwo: "eufr",
			want:     "eufr",
		},
	}

	incidentMap := mapIncidentsToLocAndISP(generateTestIncidents())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !reflect.DeepEqual(incidentMap[tt.inputTwo][tt.input][0].Location, tt.want) {
				t.Errorf("map[keyLocation][keyISP]'s Location = %v, want %v", incidentMap[tt.inputTwo][tt.input][0].Location, tt.want)
			}
		})
	}
}

func Test_incidentsMemPlacerTwo(t *testing.T) {

	tests := []struct {
		name     string
		input    string
		inputTwo string
		want     string
	}{
		{
			name:     "A location with an ISP w/ two incidents",
			input:    "AS10774x",
			inputTwo: "eufr",
			want:     "eufr",
		},
	}

	incidentMap := mapIncidentsToLocAndISP(generateTestIncidents())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !reflect.DeepEqual(incidentMap[tt.inputTwo][tt.input][0].Location, tt.want) && !reflect.DeepEqual(incidentMap[tt.inputTwo][tt.input][1].Location, tt.want) {
				t.Errorf("incidentMap[eufr][AS10774x][0]= %v, want %v", incidentMap[tt.inputTwo][tt.input][0].Location, tt.want)
			}
		})
	}
}
