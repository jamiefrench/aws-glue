# AWS Glue Job - Overlap

AWS Glue script to perform a left semi join between two datasets based on a common field, in this instance `email address`. 
Outputs the left matching results to S3. The results help to determine the overlap between datasets.

- Upload custom tranformation `aws s3 cp ./transforms/ s3://aws-glue-assets-<accountid>-<region>/transforms --recursive --exclude "*" --include "caseTransform*" --exclude "*/*"`
- Upload `job.py` via the AWS Console setting the required jobs details