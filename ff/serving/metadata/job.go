package metadata

import (
	"fmt"
)

type JobType string

const (
	EXAMPLE_JOB JobType = "Example Job"
)

type EtcdStorage struct {
	ResourceType ResourceType //Resource Type. For use when getting stored keys
	StorageType  StorageType  //Type of storage. Resource or Job
	Message      []byte       //Contents to be stored
	JobType      JobType
}

func (config EtcdConfig) ParseJob(res EtcdStorage, jobType Job) (Job, error) {
	if res.StorageType != JOB {
		return nil, fmt.Errorf("payload is not job type")
	}
	if err := json.Unmarshal(res.Message, jobType); err != nil {
		return nil, err
	}
	return jobType
	
}

func (lookup etcdResourceLookup) findJobType(t JobType) (Job, error) {
	var job Job
	switch t {
	case EXAMPLE_JOB:
		job = &ExampleJob{}
	}
	return job
}


func (lookup etcdResourceLookup) ListJobs() ([]Job, error) {
	jobs := make([]Job, 0)
	resp, err := lookup.connection.GetWithPrefix("")
	if err != nil {
		return nil, err
	}
	for _, res := range resp {
		etcdStore, err := lookup.deserialize(res)
		if err != nil {
			return nil, err
		}
		if etcdStore.StorageType == StorageType.JOB {
			job, err := lookup.connection.ParseJob(etcdStore)
			jobs = append(jobs, job)
		}
	}
	return jobs, nil
}

func (lookup etcdResourceLookup) serializeJob(job Job) ([]byte, error) {
	msg := EtcdStorage{
		Message:      job.Serialize(),
		StorageType:  JOB,
		JobType: job.JobType(),
	}
	strmsg, err := json.Marshal(msg)
	if err != nil {
		return strmsg, err
	}
	return strmsg, nil
}

func (lookup etcdResourceLookup) LookupJob(id ResourceID) (Job, error) {
	resp, err := lookup.connection.Get(id)
	if err != nil {
		return nil, &JobNotFound{id}
	}
	etcdStor, err := lookup.deserialize(resp)
	if err != nil {
		return nil, err
	}
	jobType, err := lookup.findJobType(etscStor.JobType)
	if err != nil {
		return nil, err
	}
	job, err := lookup.connection.ParseJobetscStor, jobType)
	if err != nil {
		return nil, err
	}
	return job, nil
}

func (lookup etcdResourceLookup) SetJob(id JobID, job Job) error {
	serRes, err := lookup.serializeJob(job)
	err = lookup.connection.Put(id, string(serRes))
	if err != nil {
		return err
	}
	return nil

}