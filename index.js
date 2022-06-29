var AWS = require('aws-sdk');
var s3 = new AWS.S3();
var sns = new AWS.SNS();
var dynamoDB = new AWS.DynamoDB();

const tableName = "DocumentTable";
const bucketName = 'documentlogbucket';

// Set region
AWS.config.update({region: '<your region>'});

// Create publish parameters
var snsparams = {
  Message: 'MESSAGE_TEXT', /* required */
  TopicArn: 'arn:aws:sns:ap-south-1:<account-no>:DocumentTopic'
};

// Dynamo Db parameters
var params = {
  Item: {
    DocumentNumber: {
      S: "" // String value.
    },
    PartyDetails: {
      S: "" // String value.
    }
  },
  ReturnConsumedCapacity: "TOTAL",
  TableName: tableName,
};

//extraction parameters 
 let param1 = process.env.PARAM1;
 let param2 = process.env.PARAM2;
 
exports.handler = async (event, context) => {
    console.log('Received event:', JSON.stringify(event, null, 2));
    var receivedMsg = event.body+'';
    let body;
    let statusCode = '200';
    const headers = {
        'Content-Type': 'application/json',
    };
    
    try {
        switch (event.httpMethod) {
            case 'POST':
                var docNo, party;
              //extraction logic
                if(!(receivedMsg.includes(param1)&&receivedMsg.includes(param2))){
                    throw new Error('Message Format Error');
                }
                var splitMsg = receivedMsg.split('\n');
                splitMsg.forEach(element => {
                  if(element.includes(param1)){
                     docNo = element.split(':');
                  }else if(element.includes(param2)){
                      party = element.split(':');
                  }
                });
				//log to S3
				var s3params = { 'Bucket': bucketName, 'Key': docNo[1]+".txt", 'Body': receivedMsg };
				await s3.putObject(s3params).promise();
				console.log("Successfully saved object to " + bucketName + "/" + docNo);
				
				//save to DynamoDB
				params.Item.DocumentNumber = {S: docNo[1]};
				params.Item.PartyDetails={S: party[1]};
                await dynamoDB.putItem(params).promise();
                body = "Message is saved succesfully";
                console.log("Successfully saved to DDB");
				break;
			
            default:
                throw new Error(`Unsupported method "${event.httpMethod}"`);
        }
    } catch (err) {
        statusCode = '400';
        body = err.message;
		//send message to SNS for error handling
        snsparams.Message = "Failed with error :" + err.message + " Received Messge :" + receivedMsg;
          try{
               await sns.publish(snsparams).promise();
               console.log("Sent message to SNS with ID: ");
          }catch(err){
              console.error(err, err.stack);
          }
     
        
    } finally {
        body = JSON.stringify(body);
    }

    return {
        statusCode,
        body,
        headers,
    };
};
