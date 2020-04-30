package csvParser

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
	"github.com/m-lab/clinic2019/incident_viewer_demo/incident"
)

// Given a CSV file of incidents metadata and an optional integer, numIncidents, retrieve that meta data to construct incidents
// Returns an Array of constructed incidents of size numIncidents
// Returns an Array of all possible incidents if numIncidents argument is omitted
func CsvParser(filePath string, numIncidents ...int) []incident.DefaultIncident {

	var defaultIncidents []incident.DefaultIncident = make([]incident.DefaultIncident, 0)
	var rec []string
	const shortForm = "2006-01-02"

	f, err := os.Open(filePath)

	if err != nil {
		log.Fatalf("Cannot open '%s':%s\n", filePath, err.Error())
	}

	defer f.Close()

	reader := csv.NewReader(f)
	reader.Comma = ','

	if len(numIncidents) > 1 {
		log.Fatal("Please only input one integer to signify the number of incidents you would like to generate.")
	}
 
	var i = 0
	for {
		rec, err = reader.Read()
		if len(numIncidents) == 1 && i == numIncidents[0] {
			break
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)

		}

		// Given the structure of the csv file, retrieve some values
		badTimeStartString := strings.Split(rec[3], " ")
		timeStart, _ := time.Parse(shortForm, badTimeStartString[1])

		badTimeEndString := strings.Split(rec[4], " ")
		timeEnd, _ := time.Parse(shortForm, badTimeEndString[1])

		// Generally the good period starts one year prior to the start of the bad period
		goodStartTime := timeStart.AddDate(-1, 0, 0)
		goodEndTime := timeStart

		// The empty space string accounts for an empty space in the structure of the csv file entries
		severityString := strings.Split(rec[5], " ")
		severity, _ := strconv.ParseFloat(severityString[1], 64)

		avgGoodDString := strings.Split(rec[6], " ")
		avgGoodDS, _ := strconv.ParseFloat(avgGoodDString[1], 64)

		avgBadDString := strings.Split(rec[7], " ")
		avgBadDS, _ := strconv.ParseFloat(avgBadDString[1], 64)

		locationString := strings.Split(rec[2], " ")[1]

		ASN := strings.Split(rec[1], " ")[1]

		testsAffected, err := strconv.Atoi(rec[0])

		// Make an instance of a DefaultIncident that is compatible with the Incident interface
		defaultIncident := new(incident.DefaultIncident)
		defaultIncident.MakeIncidentData(goodStartTime, goodEndTime, timeStart, timeEnd, avgGoodDS, avgBadDS, ASN, locationString, severity, testsAffected)

		// Do not treat the title row (or any other row where testsAffected is not an integer) as an incident
		if err == nil {
			defaultIncidents = append(defaultIncidents, *defaultIncident)
		}
		
		i++

	}
	return defaultIncidents
}

// Converts a default incident that implements our interface to an incident object designed to exist in a json file
// Returns incident of type IncidentJsonData
func convertDefaultIncidentToIncidentJsonData(i incident.DefaultIncident) incident.IncidentJsonData {
	gpStart, gpEnd, bpStart, bpEnd, gMetric, bMetric, asn, locationString, severity, testsAffected, gpInfo, bpInfo, iInfo := i.GetIncidentData()
	
	return incident.IncidentJsonData{
		GoodPeriodStart:  gpStart, 
		GoodPeriodEnd:    gpEnd, 
		BadPeriodStart:   bpStart, 
		BadPeriodEnd:     bpEnd,
		GoodPeriodMetric: gMetric,
		BadPeriodMetric:  bMetric,
		ASN:              asn,
		Location:         locationString,
		Severity:         severity,
		NumTestsAffected: testsAffected,
		GoodPeriodInfo:   gpInfo,
		BadPeriodInfo:    bpInfo,
		IncidentInfo:     iInfo}
}

// Takes in a location code and slices it into different location levels
// Returns an array of strings, where each string represents a location
func parseLocationCode(locationCode string) []string {
	locationCodesArr := make([]string, 0)
	upToStateLevelLocationLen := 6 // Examples: nausca, nausco

	if (len(locationCode)) > upToStateLevelLocationLen {
		// Increment by 2 because location code consists of two characters at each level
		for i := 0; i < upToStateLevelLocationLen; i += 2 {
			locationCodesArr = append(locationCodesArr, locationCode[i:i+2])
		}

		locationCodesArr = append(locationCodesArr, locationCode[upToStateLevelLocationLen:])

		return locationCodesArr
	}

	for i := 0; i < len(locationCode); i = i + 2 {
		locationCodesArr = append(locationCodesArr, locationCode[i:i+2])
	}

	return locationCodesArr
}

// Checks if a specific directory, which corresponds to a specific location, has been created
// Returns a bool
func dirExists(originPath string, dir string) bool {
	actualPath := originPath + "/" + dir

	if _, err := os.Stat(actualPath); !os.IsNotExist(err) {
		return true
	}

	return false
}

// Constructs incidents file hierarchy on basis of an incident location code
// Dynamically construct a directory hierarchy and return the path where an incident ends on the disk
// Gets used by placeIncidentsInFileHierarchy function
func dynamicallyMakeDir(originPath string, locationCode string) string {
	locationCodeArr := parseLocationCode(locationCode)
	locationCodeArrLen := len(locationCodeArr)
	// Owner can read, write, execute. Everyone else can only read and execute.
	var permissionBits os.FileMode = 0755

	for i := 0; i < locationCodeArrLen; i++ {

		if !dirExists(originPath, locationCodeArr[i]) {
			err := os.MkdirAll(originPath+"/"+locationCodeArr[i], permissionBits)

			if err != nil {
				log.Fatal(err)
			}
		}

		originPath = originPath + "/" + locationCodeArr[i]
	}

	return originPath
}

// Dynamically store incidents in a file tree hierachy
// The rootPath argument specifies where the incident file hierarchy is going to sit on disk
// Example: placeIncidentsInFileHierarchy("/Users/pascoball7/Documents/test", someIncidentMap)
func placeIncidentsInFileHierarchy(rootPath string, incMap map[string]map[string][]incident.IncidentJsonData) {
	for key, value := range incMap {
		pathToAsnJsonFiles := dynamicallyMakeDir(rootPath, key)
		for asnkey, asnValue := range value {
			filePath := pathToAsnJsonFiles + "/" + asnkey + ".json"

			// Delete an incident file if it already exist from the previous run
			// Call create later. This is all to avoid anything "incremental running"
			if _, err := os.Stat(filePath); !os.IsNotExist(err) {
				removeErr := os.Remove(filePath)

				if removeErr != nil {
					log.Fatalf("failed to remove an existing: %p", removeErr)
				}
			}

			f, err := os.Create(filePath)

			if err != nil {
				log.Fatalf("failed to create a file: %p", err)
			}

			result, errorMarshal := json.Marshal(asnValue)

			if errorMarshal != nil {
				log.Fatalf("failed to marshel: %p", errorMarshal)
			}

			_, errWrite := f.Write(result)

			if errWrite != nil {
				log.Fatalf("failed to write to a file: %p", errWrite)
			}

			errClose := f.Close()

			if errClose != nil {
				log.Fatalf("failed to close a file: %p", errClose)

			}
		}
	}
}

// Places an array of incidents in a map with every location having an asn map of incidents mapped to it
func mapIncidentsToLocAndISP(incArr []incident.DefaultIncident) map[string]map[string][]incident.IncidentJsonData {

	incidentsMemMap := make(map[string]map[string][]incident.IncidentJsonData)
	incNum := len(incArr)
	var incidentsArr []incident.IncidentJsonData = make([]incident.IncidentJsonData, 0)

	for i := 0; i < incNum; i++ {

		// New location
		_, _, _, _, _, _, asn, location, _, _, _, _, _ := incArr[i].GetIncidentData()
		_, found := incidentsMemMap[location]
		if !found {
			incidentsAsnMap := make(map[string][]incident.IncidentJsonData)
			incidentsAsnMap[asn] = append(incidentsArr, convertDefaultIncidentToIncidentJsonData(incArr[i]))
			incidentsMemMap[location] = incidentsAsnMap

		} else {
			// New asn within an existing location
			_, valFound := incidentsMemMap[location][asn]
			if !valFound {
				incidentsMemMap[location][asn] = append(incidentsArr, convertDefaultIncidentToIncidentJsonData(incArr[i]))
			
			// Already existing asn within an exisiting location
			} else {
				incidentsMemMap[location][asn] = append(incidentsMemMap[location][asn], convertDefaultIncidentToIncidentJsonData(incArr[i]))
			}
		}
	}

	return incidentsMemMap
}

// Return a list of all file and directory names in the given path
func readRootDir(rootPath string) []string {
	file, err := os.Open(rootPath)
	if err != nil {
		log.Fatalf("failed opening directory: %s", err)
	}
	defer file.Close()

	list, _ := file.Readdirnames(0) // 0 to read all files and folders

	return list
}

// A wrapper function that takes a csv file of incidents metadata and
// a root path to create a file hierarchy of incidents where directories
// represents location in internet topography and files have incidents mapped to
// a specific ISP and location
// Root path can be "/Users/[username]/Desktop/incidents/"
func CreateHierarchy(rootPath string, incidentsCSVfilePath string, bucketName string) {
	incidentArray := CsvParser(incidentsCSVfilePath)
	incidentMap := mapIncidentsToLocAndISP(incidentArray)
	placeIncidentsInFileHierarchy(rootPath, incidentMap)
	list := readRootDir(rootPath)

	for _, name := range list {
		// Upload file structure to Google Cloud Storage using the command line gsutil tool
		dirUploadCmd := "gsutil -m cp -r " + rootPath + name + " gs://" + bucketName
		cmd := exec.Command("bash", "-c", dirUploadCmd)
		// Uncomment the following lines to see progress of upload
		// cmd.Stdout = os.Stdout
		// cmd.Stderr = os.Stderr
		cmd.Run()
	}
}
