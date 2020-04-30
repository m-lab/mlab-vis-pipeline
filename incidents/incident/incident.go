package incident

import (
	"strconv"
	"time"
)

/* All incident types must implement the Incident interface */
type Incident interface {
	MakeIncidentData(goodStartTime time.Time, goodEndTime time.Time,
		badStartTime time.Time,
		badEndTime time.Time,
		avgGoodDS float64,
		avgBadDS float64,
		asn string,
		location string,
		severity float64,
		testsAffected int)
	GetIncidentData() (time.Time, time.Time, time.Time, time.Time, float64, float64, string, string, float64, int, string, string, string)
}

/* This is the incident format for the JSON file */
type IncidentJsonData struct {
	GoodPeriodStart  time.Time `json:"goodPeriodStart"`
	GoodPeriodEnd    time.Time `json:"goodPeriodEnd"`
	BadPeriodStart   time.Time `json:"badPeriodStart"`
	BadPeriodEnd     time.Time `json:"badPeriodEnd"`
	GoodPeriodMetric float64   `json:"goodPeriodMetric"`
	BadPeriodMetric  float64   `json:"badPeriodMetric"`
	ASN              string    `json:"asn"`
	Location         string    `json:"location"`
	Severity         float64   `json:"severity"`
	NumTestsAffected int       `json:"numTestsAffected"`
	GoodPeriodInfo   string    `json:"goodPeriodInfo"`
	BadPeriodInfo    string    `json:"badPeriodInfo"`
	IncidentInfo     string    `json:"incidentInfo"`
}

/* An incident for a 30% or more drop in download speed over a period of a year or longer */
type DefaultIncident struct {
	goodStartTime    time.Time
	goodEndTime      time.Time
	badStartTime     time.Time
	badEndTime       time.Time
	avgGoodDS        float64
	avgBadDS         float64
	asn              string
	location         string
	severity         float64
	numTestsAffected int
	goodPeriodInfo   string
	badPeriodInfo    string
	incidentInfo     string
}

/* Assign data members and set appropriate text for information fields */
func (i *DefaultIncident) MakeIncidentData(goodStartTime time.Time, goodEndTime time.Time,
	badStartTime time.Time,
	badEndTime time.Time,
	avgGoodDS float64,
	avgBadDS float64,
	asn string,
	location string,
	severity float64,
	testsAffected int) {

	// Convert strings to be float64 types described by 2 decimal places
	// The 'f' specifier indicates that there is no exponent in the original
	// floating point number
	gds := strconv.FormatFloat(avgGoodDS, 'f', 2, 64)
	bds := strconv.FormatFloat(avgBadDS, 'f', 2, 64)
	s := strconv.FormatFloat(severity*100, 'f', 2, 64)
	ta := strconv.Itoa(testsAffected)

	i.goodStartTime = goodStartTime
	i.goodEndTime = goodEndTime
	i.badStartTime = badStartTime
	i.badEndTime = badEndTime
	i.avgGoodDS = avgGoodDS
	i.avgBadDS = avgBadDS
	i.asn = asn
	i.location = location
	i.severity = severity
	i.numTestsAffected = testsAffected
	i.goodPeriodInfo = "Average download speed: " + gds + " mb/s"
	i.badPeriodInfo = "Average download speed: " + bds + " mb/s"
	i.incidentInfo = "Download speed dropped by " + s + "% affecting " + ta + " tests"
}

/* Retrieve all Incident fields */
func (i *DefaultIncident) GetIncidentData() (time.Time, time.Time, time.Time, time.Time, float64, float64, string, string, float64, int, string, string, string) {
	return i.goodStartTime, i.goodEndTime, i.badStartTime, i.badEndTime, i.avgGoodDS, i.avgBadDS, i.asn, i.location, i.severity, i.numTestsAffected, i.goodPeriodInfo, i.badPeriodInfo, i.incidentInfo
}
