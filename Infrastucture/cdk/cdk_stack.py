from os import path
from aws_cdk import (aws_s3, Stack, aws_lambda, aws_iam, Duration, aws_glue, aws_s3_assets as s3_assets,
                     aws_s3_deployment as s3_deployment)
from constructs import Construct
import os
from os import path



class DemoStack(Stack):
    def __init__(self, scope: Construct, id:str, environment:None, **kwargs) -> None:
        super().__init__(scope,id,**kwargs)

        bucket = aws_s3.Bucket(
            self,
            "%s-s3" % id,
            bucket_name="%s-s3" % id,
        )

        # Upload the Glue job script to S3
        script_asset = s3_assets.Asset(self, "GlueJobScript",
                                       path="glue_jobs/glue_script.py"
                                       )

        # Upload the Glue job script to S3
        deployment = s3_deployment.BucketDeployment(self, "DeployGlueJobScript",
            destination_bucket=bucket,
            sources=[s3_deployment.Source.asset("glue_jobs")],  # Path to the local script directory
        )

        #
        # # Create a Glue Database
        # database = aws_glue.CfnDatabase(self, "MyDatabase",
        #     catalog_id=self.account,
        #     database_input=aws_glue.CfnDatabase.DatabaseInputProperty(
        #         name="etl_pipline"
        #     )
        # )
        #
        # # Create an IAM Role for Glue Crawler
        # crawler_role = aws_iam.Role(self, "GlueCrawlerRole",
        #                         assumed_by=aws_iam.ServicePrincipal("glue.amazonaws.com"),
        #                         managed_policies=[
        #                             aws_iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
        #                         ]
        #                         )
        #
        # # Attach inline policy to allow the role to access the S3 bucket
        # crawler_role.add_to_policy(
        #     aws_iam.PolicyStatement(
        #         actions=["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
        #         resources=[bucket.bucket_arn, f"{bucket.bucket_arn}/*"]
        #     )
        # )

        # # Create a Glue Crawler
        # crawler = aws_glue.CfnCrawler(self, "MyCrawler",
        #     role=crawler_role.role_arn,  # Replace with your IAM role ARN
        #     database_name=database.ref,
        #     targets=aws_glue.CfnCrawler.TargetsProperty(
        #         s3_targets=[aws_glue.CfnCrawler.S3TargetProperty(
        #             path=f"s3://{bucket.bucket_name}/"
        #         )]
        #     ),
        #     table_prefix="my_",
        #     schema_change_policy=aws_glue.CfnCrawler.SchemaChangePolicyProperty(
        #         update_behavior="UPDATE_IN_DATABASE",
        #         delete_behavior="DEPRECATE_IN_DATABASE"
        #     ),
        #     crawler_security_configuration=None,
        #     configuration=None,
        #     description="My Glue Crawler"
        # )

