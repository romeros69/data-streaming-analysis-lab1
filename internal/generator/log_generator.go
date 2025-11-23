package generator

import (
	cryptorand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/romeros69/data-streaming-analysis-lab1/internal/config"
)

type LogEntry struct {
	Timestamp  string  `json:"timestamp"`
	Level      string  `json:"level"`
	Msg        string  `json:"msg"`
	RequestID  string  `json:"request_id"`
	RemoteHost string  `json:"remote_host"`
	Method     string  `json:"method"`
	Host       string  `json:"host"`
	URI        string  `json:"uri"`
	Namespace  string  `json:"namespace"`
	Duration   float64 `json:"duration"`
	API        string  `json:"api"`
	User       string  `json:"user"`
	Status     int     `json:"status"`
	Bucket     string  `json:"bucket,omitempty"`
	Object     string  `json:"object,omitempty"`
}

type OperationInfo struct {
	API       string
	Method    string
	HasBucket bool
	HasObject bool
}

type LogGenerator struct {
	cfg              *config.Config
	rng              *rand.Rand
	operations       []OperationInfo
	operationWeights []float64
	totalWeight      float64
	buckets          []string
	users            []string
	hosts            []string
	ips              []string
}

func NewLogGenerator(cfg *config.Config) *LogGenerator {
	lg := &LogGenerator{
		cfg: cfg,
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}

	lg.initOperations()
	lg.initData()

	return lg
}

func (lg *LogGenerator) initOperations() {
	operations := []OperationInfo{
		{"GetObject", "GET", true, true},
		{"PutObject", "PUT", true, true},
		{"DeleteObject", "DELETE", true, true},
		{"HeadObject", "HEAD", true, true},
		{"CopyObject", "PUT", true, true},
		{"ListParts", "GET", true, true},
		{"CreateMultipartUpload", "POST", true, true},
		{"UploadPart", "PUT", true, true},
		{"CompleteMultipartUpload", "POST", true, true},
		{"AbortMultipartUpload", "DELETE", true, true},
		{"ListBuckets", "GET", false, false},
		{"ListObjectsV2", "GET", true, false},
		{"ListObjectsV1", "GET", true, false},
		{"CreateBucket", "PUT", true, false},
		{"DeleteBucket", "DELETE", true, false},
		{"HeadBucket", "HEAD", true, false},
	}

	lg.operations = operations
	lg.operationWeights = make([]float64, len(operations))
	lg.totalWeight = 0

	for i, op := range operations {
		weight := lg.cfg.Generator.OperationWeights[op.API]
		if weight == 0 {
			weight = 1.0
		}
		lg.operationWeights[i] = weight
		lg.totalWeight += weight
	}
}

func (lg *LogGenerator) initData() {
	lg.buckets = []string{
		"data-bucket", "backup-bucket", "logs-bucket", "temp-bucket",
		"archive-bucket", "broken-shard-bucket", "slow-sync-bucket",
		"production-bucket", "staging-bucket", "test-bucket",
	}

	lg.users = make([]string, 20)
	for i := 0; i < 20; i++ {
		lg.users[i] = lg.generateUserID()
	}

	lg.hosts = []string{
		"s3.example.com", "s3-gw.production.local",
		"storage.company.com", "object-store.internal",
	}

	lg.ips = []string{
		"192.168.1.100", "10.0.0.50", "172.16.0.25",
		"10.178.152.209", "192.168.1.200", "10.0.0.75",
		"172.16.0.100", "192.168.1.150",
	}
}

func (lg *LogGenerator) GenerateLog() (*LogEntry, error) {
	op := lg.selectOperation()

	bucket := ""
	object := ""
	if op.HasBucket {
		bucket = lg.selectBucket()
		if op.HasObject {
			object = lg.generateObjectPath(bucket)
		}
	}

	problematic := lg.cfg.GetProblematicBucket(bucket)

	isError, status := lg.determineStatus(problematic)
	level := "info"
	if status >= 500 || status == 401 {
		level = "error"
	}

	duration := lg.generateDuration(isError, problematic)

	uri := lg.buildURI(op, bucket, object)

	user := lg.selectUser()

	timestamp := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")

	entry := &LogEntry{
		Timestamp:  timestamp,
		Level:      level,
		Msg:        "request",
		RequestID:  uuid.New().String(),
		RemoteHost: lg.selectIP(),
		Method:     op.Method,
		Host:       lg.selectHost(),
		URI:        uri,
		Namespace:  "",
		Duration:   math.Round(duration*1000) / 1000,
		API:        op.API,
		User:       user,
		Status:     status,
	}

	if bucket != "" {
		entry.Bucket = bucket
	}
	if object != "" {
		entry.Object = object
	}

	return entry, nil
}

func (lg *LogGenerator) selectOperation() OperationInfo {
	r := lg.rng.Float64() * lg.totalWeight
	sum := 0.0
	for i, weight := range lg.operationWeights {
		sum += weight
		if r <= sum {
			return lg.operations[i]
		}
	}
	return lg.operations[0]
}

func (lg *LogGenerator) selectBucket() string {
	return lg.buckets[lg.rng.Intn(len(lg.buckets))]
}

func (lg *LogGenerator) generateObjectPath(bucket string) string {
	paths := []string{
		"file.pdf", "image.jpg", "data.json", "video.mp4",
		"document.docx", "archive.zip", "log.txt", "backup.tar.gz",
		"path/to/file.txt", "uploads/2024/01/image.png",
		"legacy/old-file.dat", "temp/data.bin",
	}
	return paths[lg.rng.Intn(len(paths))]
}

func (lg *LogGenerator) determineStatus(problematic *config.ProblematicBucket) (bool, int) {
	errorPercent := lg.cfg.Generator.Defaults.ErrorPercent

	if problematic != nil {
		errorPercent *= problematic.ErrorMultiplier
	}

	if lg.rng.Float64()*100 < errorPercent {
		dist := lg.cfg.Generator.Defaults.ErrorStatusDistribution
		if problematic != nil && len(problematic.ErrorStatusDistribution) > 0 {
			dist = problematic.ErrorStatusDistribution
		}

		status := lg.selectStatusFromDistribution(dist)
		return true, status
	}

	status := 200
	if lg.rng.Float64() < 0.1 {
		status = 204
	}
	return false, status
}

func (lg *LogGenerator) selectStatusFromDistribution(dist map[int]float64) int {
	total := 0.0
	for _, v := range dist {
		total += v
	}

	r := lg.rng.Float64() * total
	sum := 0.0
	for status, weight := range dist {
		sum += weight
		if r <= sum {
			return status
		}
	}

	for status := range dist {
		return status
	}
	return 500
}

func (lg *LogGenerator) generateDuration(isError bool, problematic *config.ProblematicBucket) float64 {
	var min, max float64
	if isError {
		min = lg.cfg.Generator.Defaults.Duration.ErrorMin
		max = lg.cfg.Generator.Defaults.Duration.ErrorMax
	} else {
		min = lg.cfg.Generator.Defaults.Duration.SuccessMin
		max = lg.cfg.Generator.Defaults.Duration.SuccessMax
	}

	duration := min + lg.rng.Float64()*(max-min)

	if problematic != nil {
		duration *= problematic.DurationMultiplier
	}

	return duration
}

func (lg *LogGenerator) buildURI(op OperationInfo, bucket, object string) string {
	if !op.HasBucket {
		return "/"
	}

	uri := "/" + bucket
	if op.HasObject && object != "" {
		uri += "/" + object
	}

	if op.API == "ListObjectsV2" {
		uri += "?list-type=2&max-keys=1000"
	} else if op.API == "ListObjectsV1" {
		uri += "?max-keys=1000"
	} else if op.API == "UploadPart" {
		uri += fmt.Sprintf("?partNumber=%d&uploadId=%s",
			lg.rng.Intn(10)+1, uuid.New().String())
	} else if op.API == "CreateMultipartUpload" {
		uri += "?uploads"
	} else if op.API == "CompleteMultipartUpload" || op.API == "AbortMultipartUpload" {
		uri += fmt.Sprintf("?uploadId=%s", uuid.New().String())
	}

	return uri
}

func (lg *LogGenerator) selectUser() string {
	return lg.users[lg.rng.Intn(len(lg.users))]
}

func (lg *LogGenerator) selectHost() string {
	return lg.hosts[lg.rng.Intn(len(lg.hosts))]
}

func (lg *LogGenerator) selectIP() string {
	return lg.ips[lg.rng.Intn(len(lg.ips))]
}

func (lg *LogGenerator) generateUserID() string {
	bytes := make([]byte, 32)
	cryptorand.Read(bytes)
	return hex.EncodeToString(bytes)
}

func (e *LogEntry) ToJSON() (string, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (e *LogEntry) ToText() string {
	return fmt.Sprintf(
		"%s\t%s\t%s\trequest_id=%s\tapi=%s\tbucket=%s\tobject=%s\tstatus=%d\tduration=%.3f",
		e.Timestamp,
		e.Level,
		e.Msg,
		e.RequestID,
		e.API,
		e.Bucket,
		e.Object,
		e.Status,
		e.Duration,
	)
}
