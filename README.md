# filepublisher
An AppEngine handler that moves files from one CloudStorage location to another and publishers the names of those files to PubSub.

# Why is this useful?
Consider the situation where a cron job is copying files to a bucket on Google Cloud Storage. You want to process those files, and you need some way of knowing which ones are new. You also want to make sure that they only get processed once. One way to achieve that is to move the files from the incoming directory to a processing directory. When a file is moved to the processing directory, an event is published to a Google PubSub Topic with the name of the file (in its new location). This way you can have mutiple subscribers to the queue and each one of them can process a file.

# Usage
The handler takes the following query-string parameters:

| Paramter | Description |
| ---      | ---         |
| topic    | The name of the PubSub topic that events should be published to |
| dst_bucket | The bucket the files should be moved to |
| dst_path | The path in the destination bucket where files should be copied to |
| src_bucket | The bucket where files will be looked for (can be same as destination bucket) |
| src_prefix | A prefix used to find the files |

For example:
https://my-project.appstop.com/tasks/filepublisher?topic=freshfiles&dst_bucket=my-log-files&dst_path=processing&src_bucket=my-log-files&src_prefix=inbound

This URL would copy all files matching gs://my-log-files/inbound\* to gs://my-log-files/processing/ and publish a message on the PubSub topic named "freshfiles" for each one.

# ToDO
* There is a dry-run parameter that needs to be implemented.
* Needs unit tests.

# Development
The handler can be run locally using "goapp serve" by updating the app.yaml file and adding project information.
