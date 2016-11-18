package filepublisher

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
)

type F struct {
	Name    string `json:"name"`
	Size    int64  `json:"size"`
	NewName string `json:"new_name"`
}

type Job struct {
	Topic     string `json:"topic"`
	DstBucket string `json:"dst_bucket"`
	DstPath   string `json:"dst_path"`
	SrcBucket string `json:"src_bucket"`
	SrcPrefix string `json:"src_prefix"`
	DryRun    bool   `json:"dry_run"`

	Errors []string `json:"errors"`
	Files  []F      `json:"files"`

	Context   context.Context `json:"-"`
	GcsClient *storage.Client `json:"-"`
	PsClient  *pubsub.Client  `json:"-"`
	PsTopic   *pubsub.Topic   `json:"-"`
}

func init() {
	http.HandleFunc("/tasks/filepublisher", handler)
}

func handler(w http.ResponseWriter, r *http.Request) {
	qs := r.URL.Query()

	job := &Job{
		Topic:     qs.Get("topic"),
		DstBucket: qs.Get("dst_bucket"),
		DstPath:   qs.Get("dst_path"),
		SrcBucket: qs.Get("src_bucket"),
		SrcPrefix: qs.Get("src_prefix"),
		DryRun:    false,
		Context:   appengine.NewContext(r),
	}
	defer logRequest(job)
	defer respond(job, w)

	// TODO(joelpm): Make dry-run actually work.
	if dr := qs.Get("dry_run"); dr != "" {
		if drb, err := strconv.ParseBool(dr); err != nil {
			job.Errors = append(job.Errors, fmt.Sprintf("error with dry_run param: %v", err))
			return
		} else {
			job.DryRun = drb
		}
	}

	if client, err := storage.NewClient(job.Context); err != nil {
		job.Errors = append(job.Errors, fmt.Sprintf("cloud storage client creation error: %v", err))
		return
	} else {
		job.GcsClient = client
		defer job.GcsClient.Close()
	}

	if client, err := pubsub.NewClient(job.Context, appengine.AppID(job.Context)); err != nil {
		job.Errors = append(job.Errors, fmt.Sprintf("pubsub client creation error: %v", err))
		return
	} else {
		job.PsClient = client
		defer job.PsClient.Close()

		job.PsTopic = client.Topic(job.Topic)

		ok, err := job.PsTopic.Exists(job.Context)

		if err != nil {
			job.Errors = append(job.Errors, fmt.Sprintf("error checking if topic exists: %v", err))
			return
		}
		if !ok {
			if topic, err := client.CreateTopic(job.Context, job.Topic); err != nil {
				job.Errors = append(job.Errors, fmt.Sprintf("error creating topic %v: %v", job.Topic, err))
				return
			} else {
				job.PsTopic = topic
			}
		}
	}

	if acquireLock(job) {
		defer releaseLock(job)
		publishFiles(job)
	}
}

func respond(job *Job, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(job)
	w.WriteHeader(http.StatusOK)
}

func logRequest(job *Job) {
	v, _ := json.Marshal(job)
	log.Infof(job.Context, "%v", string(v))
}

func acquireLock(job *Job) bool {
	lockBucket := job.GcsClient.Bucket(job.SrcBucket)

	lockFile := lockBucket.Object("filepublisher.lock")

	if _, err := lockFile.Attrs(job.Context); err == storage.ErrObjectNotExist {
		lockWriter := lockFile.NewWriter(job.Context)
		lockWriter.ContentType = "text/plain"

		if _, err := lockWriter.Write([]byte(time.Now().String())); err != nil {
			job.Errors = append(job.Errors, fmt.Sprintf("lockfile write error: %v", err))
			return false
		}

		if err := lockWriter.Close(); err != nil {
			job.Errors = append(job.Errors, fmt.Sprintf("lockfile close error: %v", err))
			return false
		}

		return true
	} else {

		if err != nil {
			job.Errors = append(job.Errors, fmt.Sprintf("lockfile existance check error: %v", err))
		}

		return false
	}
}

func releaseLock(job *Job) {
	lockBucket := job.GcsClient.Bucket(job.SrcBucket)
	if err := lockBucket.Object("filepublisher.lock").Delete(job.Context); err != nil {
		job.Errors = append(job.Errors, fmt.Sprintf("lockfile delete error: %v", err))
	}
}

func publishFiles(job *Job) {

	srcBucket := job.GcsClient.Bucket(job.SrcBucket)
	dstBucket := job.GcsClient.Bucket(job.DstBucket)

	query := &storage.Query{}
	if job.SrcPrefix != "" {
		query.Prefix = job.SrcPrefix
	}

	it := srcBucket.Objects(job.Context, query)

	for {
		obj, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			job.Errors = append(job.Errors, fmt.Sprintf("srcBucket iterator error: %v", err))
			break
		}

		_, file := filepath.Split(obj.Name)

		newName := filepath.Join(job.DstPath, file)

		newObj, err := dstBucket.Object(newName).CopierFrom(srcBucket.Object(obj.Name)).Run(job.Context)
		if err != nil {
			job.Errors = append(job.Errors, fmt.Sprintf("error copying %v to %v: %v", obj.Name, newName, err))
			continue
		}

		if _, err = job.PsTopic.Publish(job.Context, &pubsub.Message{Data: []byte(newName)}); err != nil {
			job.Errors = append(job.Errors, fmt.Sprintf("error publishing to topic for %v: %v", newName, err))
			if dstBucket.Object(newName).Delete(job.Context); err != nil {
				job.Errors = append(job.Errors, fmt.Sprintf("error cleaning up unpublished file %v: %v", newName, err))
			}
			continue
		}

		if err := srcBucket.Object(obj.Name).Delete(job.Context); err != nil {
			job.Errors = append(job.Errors, fmt.Sprintf("error deleting original file %v: %v", obj.Name, err))
		}

		job.Files = append(job.Files, F{obj.Name, obj.Size, newObj.Name})
	}

}
