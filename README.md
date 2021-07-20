# Spark Java Application For Moving S3 Objects In Parallel With Filtering

A Spark Java application that can be used to move large amounts of S3 Objects in parallel from one S3 location to another by specifying source and target buckets and prefixes. The application leverages Java multithreading to speed up the process of moving the objects. The user can also specify a series of filters on the objects to be moved, such as filters on object title and size.

## Building
Before building the maven project, you need to set the AWS_REGION and AWS_PROFILE environment variables.
```bash
# set the aws region and the aws profile (profile is set in ~/.aws/credentials)
export AWS_REGION=<AWS REGION>
export AWS_PROFILE=<YOUR IAM USER PROFILE NAME>
mvn clean install
```

Supported REST Endpoints:

/health (GET):
    Description: Health Check for app.

    Request: Void

    Response:
             {
               "status": "Healthy"
             }

 /s3-move (POST):
    Description: Parallel move s3 objects with filtering

    Request:
            /s3-move

            Request Body:
            {
                "s3-source-bucket":"",
                "s3-source-prefix":"",
                "s3-target-bucket":"",
                "s3-target-prefix":"",
                "filter-names":[],
                "filter-min-file-size":0,
                "filter-max-file-size":100000000000
            }

    Response:
            {
                "movedFiles": [
                    "s3-source-prefix/..",
                    "s3-source-prefix/..",
                    "s3-source-prefix/..",
                    "s3-source-prefix/.."
                ],
                "successfulMove": "True"
            }

## Contributors

Chris M. - @mtcheli
Leonardo K. - @kanagusk
Pranay S. - @pranays
Zoran I. - @zorani

## License

This library is licensed under the MIT-0 License. See the LICENSE file.