import json
import boto3
import logging
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr
from boto3.dynamodb.conditions import Key
from datetime import datetime


logger = logging.getLogger()
logger.setLevel(logging.INFO)

#Declaring DynamoDB table Information
dynamodbTableName = 'ProductInventory'
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamodbTableName)

#Declaring HTTP methods and Paths
getMethod = 'GET'
postMethod = 'POST'
patchMethod = 'PATCH'
deleteMethod = 'DELETE'
healthPath = '/health'
productPath = '/product'
productsPath = '/products' 
        
def lambda_handler(event, context):
    httpmethod = event['httpMethod']
    path = event['path']
    print("HTTP METHOD:",httpmethod)
    print("HTTP PATH:",path)
    if ( httpmethod == getMethod and path == healthPath ):
        response = buildresponse(200, 'Success')
    elif ( httpmethod == getMethod and path == productPath ):
        dict=event['queryStringParameters']
        if len(dict) == 1:
            category=event['queryStringParameters']['category']
            response=getProductBasedOnCategory(category)
        elif len(dict) == 2:
            email=event['queryStringParameters']['email']
            if 'category' in event['queryStringParameters'].keys():
                print("category")
                category=event['queryStringParameters']['category']
                response = getProductBasedOnEmailCategory(email,category) 
            if 'date' in event['queryStringParameters'].keys():
                date=event['queryStringParameters']['date']
                response = getProductBasedOnEmailDate(email,date)
        elif len(dict) == 3:
            Email=event['queryStringParameters']['email']
            Date=event['queryStringParameters']['date']
            Time=event['queryStringParameters']['time']
            response = getProductBasedOnEmailDateTime(Email,Date,Time)
        else:
            response = buildresponse(404, 'Not Found')
    elif ( httpmethod == getMethod and path == productsPath ):
        response = getProducts()
    elif ( httpmethod == postMethod and path == productPath ):
        requestBody = json.loads(event['body'])
        response = postProducts(requestBody)
    elif ( httpmethod == deleteMethod and path == productPath ):
        deleteItem=event['queryStringParameters']['email']
        print("delete item is",deleteItem )
        response = deleteProducts(deleteItem)
    elif ( httpmethod == patchMethod and path == productPath):
        requestBody = json.loads(event['body'])
        response = updateProducts(requestBody)
    else:
        response = buildresponse(404, 'Not Found')
    return response
 
#------------------------------------------------------------                
#CREATE a record
def postProducts(requestBody):
    try:
        item=requestBody
        table.put_item(Item=item)
        body = {
            'OPERATION': 'POST',
            'MESSAGE': 'SUCCESS',
            'ITEM': item
        }
        return buildresponse(200, body)
    except:
        logger.exception ("UNABLE TO POST ITEMS")
        
#------------------------------------------------------------ 
#DELETE a record
def deleteProducts(email):
    try:
        response = table.delete_item(
            Key={
                'ProductPurchaserEmail': email
            }
        )
        body = {
            'OPERATION': 'DELETE',
            'MESSAGE': 'SUCCESS',
        }
        return buildresponse(200, body)
            
    except:
        logger.exception (" not able to  delete the item")
        
#------------------------------------------------------------ 
#UPDATE a record
def updateProducts(requestBody):
    try:
        item=requestBody
        email=item['email']
        updatekey= item['updatekey']
        updateValue=item['updatevalue']
        table.update_item(
            Key={
               'ProductPurchaserEmail': email
            },
            UpdateExpression='SET %s = :value' % updatekey,
            ExpressionAttributeValues={
                ':value': updateValue
            }
        )
        body = {
            'OPERATION': 'UPDATE',
            'MESSAGE': 'SUCCESS'
        }
        return buildresponse(200, body)
    except:
        logger.exception ("Unable to update the item")

 
#------------------------------------------------------------  
#SEARCH FOR A RECORD
#based on and date

def getProductBasedOnCategory(category):
    try:
        categoryname=category
        response = table.query(
            IndexName="ProductCategory-index",
            KeyConditionExpression=Key('ProductCategory').eq(categoryname)
        )
        if 'Items' in response:
            body = {
                'OPERATION': 'SEARCH',
                'STATUS':'SUCCESS',
                'ITEMS': response['Items']
            }
            return buildresponse(200, body)
        else:
            return buildresponse(404, {'Message': 'ProductCategory: %s not found' % ProductCategory})
    except:
            logger.exception (" not able to retrive the item")

#------------------------------------------------------------  
#SEARCH FOR A RECORD
#based on email and category
def getProductBasedOnEmailCategory(Email, Category):
    try:
        purchaseremail=Email
        categoryname=Category 
        print(purchaseremail)
        print(categoryname)
        response = table.query(
           FilterExpression=Attr('ProductCategory').eq(categoryname),
           KeyConditionExpression=Key('ProductPurchaserEmail').eq(purchaseremail)
        )
        if 'Items' in response:
            body = {
                'OPERATION': 'SEARCH',
                'STATUS':'SUCCESS',
                'ITEMS': response['Items']
            }
            return buildresponse(200, body)
        else:
            return buildresponse(404, {'Message': 'Category: %s not found' % Category})
            
    except:
            logger.exception (" not able to retrive the item")
#------------------------------------------------------------                 
#SEARCH FOR A RECORD
#based on email and date


def getProductBasedOnEmailDate(Email, Date):
    try:
        email=Email
        date=Date
        response = table.query(
           FilterExpression=Attr('ProductPurchaseTimestamp').begins_with(date),
           KeyConditionExpression=Key('ProductPurchaserEmail').eq(email)
        )
        if 'Items' in response:
            body = {
                'OPERATION': 'SEARCH',
                'STATUS':'SUCCESS',
                'ITEMS': response['Items']
            }
            return buildresponse(200, body)
        else:
            return buildresponse(404, {'Message': 'DATE: %s not found, enter DATE in YYYY:MM:YY format' %  Date})
            
    except:
            logger.exception (" not able to retrive the item")

#------------------------------------------------------------ 
#------------------------------------------------------------                 
#SEARCH FOR A RECORD
#based on email date and Time
#DATE TIME FROMAT: YYYY:MM:YY HH:MM

def getProductBasedOnEmailDateTime(Email,Date,Time):
    try:
        email=Email
        date=Date
        time=Time
        t="T"
        date_time= date+t+time
        print(date_time)
        response = table.query(
           FilterExpression=Attr('ProductPurchaseTimestamp').contains(Time),
           KeyConditionExpression=Key('ProductPurchaserEmail').eq(email)
        )
        if 'Items' in response:
            body = {
                'OPERATION': 'SEARCH',
                'STATUS':'SUCCESS',
                'ITEMS': response['Items']
            }
            return buildresponse(200, body)
        else:
            return buildresponse(404, {'Message': 'TIME: %s not found, enter TIME in HH:MM format' %  Date})
    except:
            logger.exception (" not able to retrive the item")

#------------------------------------------------------------  
#GET ALL THE RECORDS


def getProducts():
    try:
        response = table.scan()
        result = response['Items']
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            result.extend(response['Items'])
        body = result
        return buildresponse(200, body)
        
    except:
        logger.exception (" not able to retrive the items")

#------------------------------------------------------------        
          
def buildresponse(statusCode, body):
    response = {
        'statusCode': statusCode,
        'body': body
    }
    if body is not None:
        response['body'] = json.dumps((body), cls=CustomEncoder)
    return response

#------------------------------------------------------------        


  
class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        
        return json.JSONEncoder.default(self, obj)
        
#------------------------------------------------------------        
