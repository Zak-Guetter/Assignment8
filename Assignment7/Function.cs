using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using Amazon.DynamoDBv2.DocumentModel;
using Newtonsoft.Json;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2;
using Amazon.Lambda.DynamoDBEvents;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace Assignment7
{

    public class Item
    {
        public string itemId;
        public string description;
        public int rating;
        public string type;
        public string company;
        public string lastInstanceOfWord;
    }

    public class Rating
    {
        public string type;
        public int count = 0;
        public double average = 0;
    }
    public class Function
    {

        private static AmazonDynamoDBClient client = new AmazonDynamoDBClient();
        
        /// <summary>
        /// A simple function that takes a string and does a ToUpper
        /// </summary>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<List<Item>> FunctionHandler(DynamoDBEvent input, ILambdaContext context)
        {
            Table table = Table.LoadTable(client, "items");
            List<Item> ratings = new List<Item>();
            List<DynamoDBEvent.DynamodbStreamRecord> records = (List<DynamoDBEvent.DynamodbStreamRecord>)input.Records;


            if(records.Count > 0) 
            {
                DynamoDBEvent.DynamodbStreamRecord record = records[0];
                if (record.EventName.Equals("INSERT"))
                {
                    Document myDoc = Document.FromAttributeMap(record.Dynamodb.NewImage);
                    Item myBook = JsonConvert.DeserializeObject<Item>(myDoc.ToJson());

                   
                    GetItemResponse res = await client.GetItemAsync("RatingsByType", new Dictionary<string, AttributeValue>
                        {
                            {"type", new AttributeValue { S = myBook.type} }
                        }
                    );
                    Document getDoc = Document.FromAttributeMap(res.Item);
                    Rating myItem = JsonConvert.DeserializeObject<Rating>(getDoc.ToJson());

                    double average = myBook.rating;


                    if (myItem.count.Equals("0"))
                    {
                        if(myBook.company.ToUpper() == "B")
                        {
                            average = myBook.rating / 2;
                        }
                        else
                        {
                            average = myBook.rating;
                        }
                    }
                    else
                    {
                        if (myBook.company.ToUpper() == "B") 
                        {
                            myBook.rating = myBook.rating / 2;
                            average = ((myItem.count * myItem.average) + myBook.rating) / (myItem.count + 1);
                        }
                        else
                        {
                            average = ((myItem.count * myItem.average) + myBook.rating) / (myItem.count + 1);
                        }
                    }

                    //double average = 10;

                    var request = new UpdateItemRequest
                    {
                        TableName = "RatingsByType",
                        Key = new Dictionary<string, AttributeValue>
                        {
                            {
                                "type",
                                new AttributeValue { S = myBook.type }
                            }
                        },
                        AttributeUpdates = new Dictionary<string, AttributeValueUpdate>()
                        {
                            {
                                "count",
                                new AttributeValueUpdate
                                {
                                    Action = "ADD",
                                    Value = new AttributeValue { N = "1" }
                                }
                            },
                            {
                                "average",
                                new AttributeValueUpdate
                                {
                                    Action = "PUT",
                                    Value = new AttributeValue { N = average.ToString("0.#") }
                                }
                            } 
                            
                        }
                    };
                    await client.UpdateItemAsync(request);
                }
            }
            return ratings;
        }/**          "itemId": {
            "S": "101"
          },
          "description": {
            "S": "101"
          },
          "rating": {
            "N": "10"
          },
          "type": {
            "S": "finalTest"
          },
          "company": {
            "S": "B"
          },
          "lastInstanceOfWord": {
            "S": "yes"
          },*/
    }
}
