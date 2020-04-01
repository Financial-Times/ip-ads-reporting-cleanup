# ip-ads-reporting
Queries the Adbook (Ads OMS) to delete AdRevenue and AdRevenueSchedule info from Salesforce
This is implemented with a Step function to help cope with the large volume of line items to be processed

Serverless yaml defines a state machine shown below this can be run on a schedule.
stateMachines:
    processfile:
      name: stepf-process-delete
      definition:
        Comment: "Ad-revenue deletion application"
        StartAt: Extract
        States:
          Extract:
            Type: Task
            Resource: "arn:aws:lambda:#{AWS::Region}:#{AWS::AccountId}:function:${self:service}" 
            Next: CheckResults
          Iterate:
            Type: Pass
            Result: { success: true }
            ResultPath: $.Iterate
            Next: Extract
          Done:
            Type: Pass
            End: true  
          CheckResults: 
            Type: Choice
            Choices: 
              - Variable: "$.extract.results['finished']"
                BooleanEquals: false
                Next: "Iterate"
            Default: "Done"

Due to the large number of lines to be processed a step function allows us to get more control over the execution time of the lambda.  The consuming lambda's can then be run concurrently or if lambda concurrency limits execution one can build in a wait state .  So you can run a batch then wait a specified amount of time before running the next batch, this will be defined in the step function, no code changes required.  In this case a wait has not been deemed necessary but can be built in if needing to extend the process to run for up to 24 months and if lambda concurrency limits become an issue. Again the batch size can be varied and defined in the step function as an env var rather than in the code.

stepf-process-delete kicks off on schedule with Extract, Extract is a lambda function it calls the adbook api and downloads the report from 2 months prior to today and going forward 14 months.  That file is saved to s3.   Once saved it is processed line by line to extract those line items that are yet to start.  The first step is a call to the handle-extract function, first call is made with {}. getDQTReport get called with variables for the total number of rows downloaded from adbook where the start date is >= the start of the current period (2 months prior - 14 months ahead), number of rows processed so far and an array for any errors (errors are all just sent to splunk).  getDqt downloads the file from adbook, processes the first batchsize currently 200 rows then saves it to the search folder.

stateMachines:
    processfile:
      name: stepf-process-delete
      definition:
        Comment: "Ad-revenue deletion application"
        StartAt: Extract
        States:
          Extract:
            Type: Task
            Resource: "arn:aws:lambda:#{AWS::Region}:#{AWS::AccountId}:function:${self:service}" 
            Next: CheckResults
Once the first batch of 200 is done getDQT ends the first iteration by return {{ processedRows: value, importedRows: value, errors: 'not used currently', finished: boolean } this is passed to CheckResults.

CheckResults: 
            Type: Choice
            Choices: 
              - Variable: "$.extract.results['finished']"
                BooleanEquals: false
                Next: "Iterate"
            Default: "Done"

 CheckResults will check the output for the finished status, if finished is not true it will call the Iterate step using it to pass the results from the last run and trigger the Extract again to carry on and run the next batch of 200.

 On the otherhand if finished is true the Default done is called to end the state machine.

 Iterate:
            Type: Pass
            Result: { success: true }
            ResultPath: $.Iterate
            Next: Extract

And so it continues until all lines are processed.

Once each batch file is put in the s3 bucket a put event is triggered, just as in billing.  In this case the search lambda is triggered, for each dropid listed in the file a search is carried out against Salesforce any id that is not found is flagged and written to a prepare to delete file.

Once all the id's in the file have been validated any drops found to have been removed from Adbook are written to a file.  This file will be saved to the delete folder.  The save to the s3 bucket event triggers the delete lambda which runs the job to delete the schedules identified as no longer available in Adbook.

This approach gives more flexibility and is scalable.

enhancement note: There is a jforce method which is supposed to pull parent together with all it children in one call but when I tried to use it last year it did not work it is worth investigating again to see if it now works.  So it is a select and include clause.  This will save having to make a call first to get the parent and then the children.


