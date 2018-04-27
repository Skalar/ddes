import {AwsMetaStore, AwsStore, AwsStoreConfig} from '@ddes/aws-store'

export function aws(opts: {testId: string}, config: Partial<AwsStoreConfig>) {
  const {testId} = opts

  return new AwsStore({
    tableName: `ddess-${testId}`,
    ...(!process.env.REAL_SERVICES_TEST && {
      s3ClientConfiguration: {
        endpoint: process.env.LOCAL_S3 || 'http://localhost:5000',
        sslEnabled: false,
        s3ForcePathStyle: true,
        accessKeyId: 'test',
        secretAccessKey: 'test',
        region: 'us-east-1',
      },
    }),
    ...(!process.env.REAL_SERVICES_TEST && {
      dynamodbClientConfiguration: {
        endpoint: process.env.LOCAL_DDB || 'http://localhost:8000',
        region: 'us-east-1',
        accessKeyId: 'test',
        secretAccessKey: 'test',
      },
    }),
    snapshots: {
      s3BucketName: `ddess-${testId}`,
      keyPrefix: 'snapshots/',
      manageBucket: true,
    },
    ...config,
  })
}

export function awsMeta(opts: {testId: string}) {
  const {testId} = opts

  return new AwsMetaStore({
    tableName: `ddess-${testId}-meta`,
    ...(!process.env.REAL_SERVICES_TEST && {
      dynamodbClientConfiguration: {
        endpoint: process.env.LOCAL_DDB || 'http://localhost:8000',
        region: 'us-east-1',
        accessKeyId: 'test',
        secretAccessKey: 'test',
      },
    }),
  })
}
