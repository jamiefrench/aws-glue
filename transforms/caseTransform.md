# Custom Transform 
Transform a string column's casing to uppercase or lowercase

Glue Custom Transform node allow you to perform more complicated transformations on your data. Copying the transformation files 
to a specified S3 location will enable scripts and the Visual editor to perform the custom transformation.

- Upload custom tranformation `aws s3 cp . s3://aws-glue-assets-<accountid>-<region>/transforms --recursive --exclude "*" --include "caseTransform*" --exclude "*/*"`