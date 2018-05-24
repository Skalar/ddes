import {
  AwsEventStore,
  AwsMetaStore,
  AwsSnapshotStore,
  AwsEventStoreConfig,
} from '@ddes/aws-store'

export function eventStore(
  opts: {testId: string},
  config: Partial<AwsEventStoreConfig>
) {
  const {testId} = opts

  return new AwsEventStore({
    tableName: `ddes-${testId}`,
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
    ...config,
  })
}

export function metaStore(opts: {testId: string}) {
  const {testId} = opts

  return new AwsMetaStore({
    tableName: `ddes-${testId}-meta`,
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

export function snapshotStore(opts: {testId: string}) {
  const {testId} = opts

  return new AwsSnapshotStore({
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
    bucketName: `ddes-${testId}`,
    keyPrefix: 'snapshots/',
    manageBucket: true,
  })
}
