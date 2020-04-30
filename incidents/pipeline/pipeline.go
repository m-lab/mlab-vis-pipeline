package pipeline

import (
	"os"
	"os/exec"

	"github.com/m-lab/mlab-vis-pipeline/incidents/csvParser"
)

// The unique identifier of the Google Cloud Storage bucket used to store incidents
var BUCKET_NAME = "incidents-location-hierarchy"

// The name of the CSV used to hold ungrouped incident data
var INCIDENT_CSV = "incidents.csv"

// The path where the incident hierarchy will be stored
var INCIDENT_HIERARCHY_PATH = os.Getenv("HOME") + "/Desktop/generatedIncidents/"

func findIncidents() {
	// Successfully running this requires appropriate GOOGLE_APPLICATION_CREDENTIALS
	runSignalSearcher := "go run github.com/m-lab/signal-searcher  | sort -nk1 > " + INCIDENT_CSV
	cmd := exec.Command("bash", "-c", runSignalSearcher)
	cmd.Run()
}

func mountHierarchy() {
	csvParser.CreateHierarchy(INCIDENT_HIERARCHY_PATH, INCIDENT_CSV, BUCKET_NAME)
}

func runPipeline() {
	// Generate a CSV file of incidents
	findIncidents()

	// Temporary place a directory of incidents to copy files to GCS
	mountHierarchy()

	// Remove CSV file and directory of incidents
	os.Remove(INCIDENT_CSV)
	os.RemoveAll(INCIDENT_HIERARCHY_PATH)

}
