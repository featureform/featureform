package runner

import (
	"fmt"
)

type MockCronJobClient struct {
	Namespace string
	cronJobs []CronJob
}

type MockCronJobConfig struct {
	id string
	schedule string
}

type MockCronJob struct {
	config MockCronJobConfig
	Completions []string
}

// type CronJobClient interface {
// 	GetJob(id string) (CronJob, error)
// 	ScheduleJob(config CronJobConfig) error
// 	DeleteJob(id string) error
// 	ListJobs() ([]CronJob, error)
// }

func (m MockCronJobClient) GetJob(id string) (CronJob, error) {
	for _, cronJob := range m.cronJobs {
		if cronJob.Config().ID() == id {
			return cronJob, nil
		}
	}
	return nil, fmt.Errorf("cron job %s has not been created", id)
}

func (m *MockCronJobClient) ScheduleJob(config CronJobConfig) error {
	var c CronJob = MockCronJob{config: config.(MockCronJobConfig), Completions: []string{}}
	mockList := m.cronJobs
	mockList = append(mockList, c)
	m.cronJobs = mockList
	return nil
}

func (m *MockCronJobClient) DeleteJob(id string) error {
	for i, cronJob := range m.cronJobs {
		if cronJob.Config().ID() == id {
			m.cronJobs[i] = m.cronJobs[len(m.cronJobs)-1]
			m.cronJobs = m.cronJobs[:len(m.cronJobs)-1]
			break
		}
	}
	return fmt.Errorf("cron job %s has not been created", id)
}

func (m *MockCronJobClient) ListJobs() ([]CronJob, error) {
	return m.cronJobs, nil
}

func (m MockCronJobConfig) Schedule() string {
	return m.schedule
}

func (m MockCronJobConfig) ID() string {
	return m.id
}

func (m MockCronJob) Config() CronJobConfig {
	return m.config
}

func (m MockCronJob) String() string {
	return fmt.Sprintf("%s, %v", m.config, m.Completions)
}