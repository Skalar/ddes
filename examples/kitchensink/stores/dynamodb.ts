import { DynamodbEventStore } from '@ddes/dynamodb'
import { DynamoDB } from 'aws-sdk'

export const dynamodb = new DynamodbEventStore({
	tableName: 'ddes-test',
	client: new DynamoDB({
		endpoint: 'http://localhost:8081',
		region: 'us-east-1',
		accessKeyId: 'test',
		secretAccessKey: 'test',
	}),
})
