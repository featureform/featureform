package job

import (
	"fmt"
)

type JobConfig interface {
	String() string
	Serialized() []byte
}

type ExampleJobConfig struct {
	Source string
	Dest string
}

func (e ExampleJobConfig) String() string {
	return fmt.Sprintf("Source: %s, Destination: %s", e.Source, e.Dest)
}

func (e ExampleJobConfig) Serialized() []byte {
	 b, _ := json.Marshal(e)
	return b
}

type JobStatus string

const (
	ACTIVE JobStatus = "Active"
	FAILED = "Failed"
	COMPLETED = "Completed"
)

type JobID string

type Job interface {
	ID() JobID
	JobType() string
	Status() JobStatus
	SetStatus(status JobStatus) error
	String() string
	Serialized() []byte
}

type ExampleJob struct {
	Name string
	ID string
	Status JobStatus
	Config ExampleJobConfig
}

func (e ExampleJob) ID() JobID {
	return fmt.Sprintf("%s, %s", e.Name, e.ID)
}

func (e ExampleJob) JobType() JobType {
	return EXAMPLE_JOB
}

func (e ExampleJob) Status() JobStatus {
	return e.Status
}

func (e ExampleJob) SetStatus(status JobStatus) error {
	e.Status = status
	return nil
}

func (e ExampleJob) String() string {
	return fmt.Sprintf("Job name: %s, ID: %s, Config: %s",e.Name, e.ID, e.Config.String())
}

func (e ExampleJob) Serialized() []byte {
	return json.Marshal(e)
}

func PutJob(job Job) error {
	err := config.SetJob(job.ID(), job.Serialized())
	if err != nil {
		return err
	}
}

func GetJob(id JobID) (Job, error) {
	jobBytes, err := config.LookupJob(id)
	if err != nil {
		return nil, err
	}
	job, err := ParseJob(jobBytes)
	if err != nil {
		return nil, err
	}
	return job, nil
}
	

func SetJobStatus(id JobID) error {
	job, err := GetJob(id)
	if err != nil {
		return err
	}
	
	if err := job.SetStatus(); err != nil {
		return err
	}
	
	if err := PutJob(job); err != nil {
		return err
	}
	return nil
}

func GetJobsWithStatus(status JobStatus) ([]Job, error) {
	selectedJobs := make([]Job, 0)
	jobs, err := ListJobs()
	if err != nil {
		return nil, err
	}
	for _, job := range jobs {
		if job.Status() == status {
			selectedJobs = append(selectedJobs, job)
		}
	}
	return selectedJobs, nil
		
}

func GetJobs() ([]Job, error) {
	return config.ListJobs()
}


//just need dummy implementation of Put, Lookup, and ListJobs