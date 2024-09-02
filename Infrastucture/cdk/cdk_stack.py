from os import path
from aws_cdk import (aws_s3, Stack, aws_lambda, aws_iam, Duration)
from constructs import Construct
import os
from os import path



class DemoStack(Stack):
    def __init__(self, scope: Construct, id:str, environment:None, **kwargs) -> None:
        super().__init__(scope,id,**kwargs)

        sample_bucket = aws_s3.Bucket(
            self,
            "%s-s3" % id,
            bucket_name="%s-s3" % id,
        )


