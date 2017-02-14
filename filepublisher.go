/*
An AppEngine service that copies files from one CloudStorage location to another
and publishes the names of the files (in their new location) to a PubSub topic.

This could be used as the first step of a data pipline that brings log files
into GCP. An external (to GCP) service would copy the logs into a well known
location on CloudStorage. An AppEngine cron job would make a call to this
service with the right parameters, which would then move the files to a staging
area and publish them to a queue to be consumed by other workers.

The handler will listen at:

http://PROJECT_ID.appspot.com/tasks/filepublisher

The paramters required by the handler defined in this service are:

  topic      - name of the pubsub topic to which names of staged files should be published
  dst_bucket - CloudStorage bucket name where files should be staged
  dst_path   - path in the CloudStorage bucket where files should be staged
  src_bucket - CloudStorage bucket where files can be found
  src_prefix - prefix used to identify files in the source bucket
  dry_run    - if true, will only show what action would've been taken (TBD)

An example call could look like:
  http://PROJECT_ID.appspot.com/tasks/filepublisher?topic=MY_TOPIC&dst_bucket=MY_BUCKET&dst_path=staged&src_bucket=MY_BUCKET&src_prefix=inbound

To run the service locally for development the project ID must be specified in
the environment and 'go run' can be used:
  GCLOUD_PROJECT=<PROJECT_ID> go run filepublisher.go
*/
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"google.golang.org/appengine"
)

type F struct {
	Name    string `json:"name"`
	Size    int64  `json:"size"`
	NewName string `json:"new_name"`
}

type job struct {
	Topic     string `json:"topic"`
	DstBucket string `json:"dst_bucket"`
	DstPath   string `json:"dst_path"`
	SrcBucket string `json:"src_bucket"`
	SrcPrefix string `json:"src_prefix"`
	DryRun    bool   `json:"dry_run"`

	Errors []string `json:"errors"`
	Files  []F      `json:"files"`

	context   context.Context `json:"-"`
	gcsClient *storage.Client `json:"-"`
	psClient  *pubsub.Client  `json:"-"`
	psTopic   *pubsub.Topic   `json:"-"`
}

var (
	projectId string
)

func main() {
	if projectId = os.Getenv("GCLOUD_PROJECT"); projectId != "" {
		appengine.Main()
	} else {
		log.Println("FATAL - could not get env variable 'GCLOUD_PROJECT'")
	}
}

func init() {
	http.HandleFunc("/tasks/filepublisher", handler)
}

func handler(w http.ResponseWriter, r *http.Request) {
	qs := r.URL.Query()

	//log.Println("handling 2")
	job := &job{
		Topic:     qs.Get("topic"),
		DstBucket: qs.Get("dst_bucket"),
		DstPath:   qs.Get("dst_path"),
		SrcBucket: qs.Get("src_bucket"),
		SrcPrefix: qs.Get("src_prefix"),
		DryRun:    false,
		context:   appengine.NewContext(r),
	}
	defer logRequest(job)
	defer respond(job, w)

	//log.Println("handling 3")

	// TODO(joelpm): Make dry-run actually work.
	if dr := qs.Get("dry_run"); dr != "" {
		if drb, err := strconv.ParseBool(dr); err != nil {
			job.Errors = append(job.Errors, fmt.Sprintf("error with dry_run param: %v", err))
			return
		} else {
			job.DryRun = drb
		}
	}

	//log.Println("handling 4")
	if client, err := storage.NewClient(job.context); err != nil {
		job.Errors = append(job.Errors, fmt.Sprintf("cloud storage client creation error: %v", err))
		return
	} else {
		job.gcsClient = client
		defer job.gcsClient.Close()
	}

	//log.Println("handling 5")

	if client, err := pubsub.NewClient(job.context, projectId); err != nil {
		job.Errors = append(job.Errors, fmt.Sprintf("pubsub client creation error: %v", err))
		return
	} else {
		job.psClient = client
		defer job.psClient.Close()

		//log.Println("handling 6")

		job.psTopic = client.Topic(job.Topic)

		//log.Println("handling 7")
		ok, err := job.psTopic.Exists(job.context)

		if err != nil {
			job.Errors = append(job.Errors, fmt.Sprintf("error checking if topic exists: %v", err))
			return
		}
		if !ok {
			if topic, err := client.CreateTopic(job.context, job.Topic); err != nil {
				job.Errors = append(job.Errors, fmt.Sprintf("error creating topic %v: %v", job.Topic, err))
				return
			} else {
				job.psTopic = topic
			}
		}
	}

	//log.Println("handling 8")

	if acquireLock(job) {
		defer releaseLock(job)
		//log.Println("handling 9")
		publishFiles(job)
	}
}

func respond(job *job, w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(job)
	//log.Println("respond done")
}

func logRequest(job *job) {
	v, err := json.Marshal(job)
	if err != nil {
		log.Println(err)
	}
	log.Printf("%v\n", string(v))
}

func acquireLock(job *job) bool {
	//log.Println("acquire 1")
	lockBucket := job.gcsClient.Bucket(job.SrcBucket)

	//log.Println("acquire 2")
	lockFile := lockBucket.Object("filepublisher.lock")

	if _, err := lockFile.Attrs(job.context); err == storage.ErrObjectNotExist {
		lockWriter := lockFile.NewWriter(job.context)
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

func releaseLock(job *job) {
	//log.Println("release 1")
	lockBucket := job.gcsClient.Bucket(job.SrcBucket)
	if err := lockBucket.Object("filepublisher.lock").Delete(job.context); err != nil {
		job.Errors = append(job.Errors, fmt.Sprintf("lockfile delete error: %v", err))
	}
}

func publishFiles(job *job) {
	//log.Println("publish 1")

	srcBucket := job.gcsClient.Bucket(job.SrcBucket)
	dstBucket := job.gcsClient.Bucket(job.DstBucket)

	query := &storage.Query{}
	if job.SrcPrefix != "" {
		query.Prefix = job.SrcPrefix
	}

	//log.Println("publish 2")
	it := srcBucket.Objects(job.context, query)

	//log.Println("publish 3")
	for {
		//log.Println("publish 3.1")
		obj, err := it.Next()
		//log.Println("publish 3.2")
		if err == iterator.Done {
			//log.Println("publish 3.3")
			break
		}
		if err != nil {
			//log.Println("publish 3.4")
			job.Errors = append(job.Errors, fmt.Sprintf("srcBucket iterator error: %v", err))
			break
		}

		//log.Println("publish 4")
		_, file := filepath.Split(obj.Name)

		newName := filepath.Join(job.DstPath, file)

		newObj, err := dstBucket.Object(newName).CopierFrom(srcBucket.Object(obj.Name)).Run(job.context)
		if err != nil {
			job.Errors = append(job.Errors, fmt.Sprintf("error copying %v to %v: %v", obj.Name, newName, err))
			continue
		}

		if _, err = job.psTopic.Publish(job.context, &pubsub.Message{Data: []byte(newName)}); err != nil {
			job.Errors = append(job.Errors, fmt.Sprintf("error publishing to topic for %v: %v", newName, err))
			if dstBucket.Object(newName).Delete(job.context); err != nil {
				job.Errors = append(job.Errors, fmt.Sprintf("error cleaning up unpublished file %v: %v", newName, err))
			}
			continue
		}

		if err := srcBucket.Object(obj.Name).Delete(job.context); err != nil {
			job.Errors = append(job.Errors, fmt.Sprintf("error deleting original file %v: %v", obj.Name, err))
		}

		job.Files = append(job.Files, F{obj.Name, obj.Size, newObj.Name})
	}

}
