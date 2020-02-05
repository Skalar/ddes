import {
  AwsEventStore,
  AwsMetaStore,
  AwsSnapshotStore,
  AwsEventStoreConfig,
} from '@ddes/aws-store'
import Store from './Store'

export default class AwsStores implements Store {
  constructor(private testId: string) {}

  async setup() {}

  async teardown() {}

  public eventStore(
    {testId, ...config}: Partial<AwsEventStoreConfig> & {testId?: string} = {}
  ) {
    return new AwsEventStore({
      tableName: `ddes-${testId || this.testId}`,
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
          endpoint: process.env.LOCAL_DDB || 'http://localhost:8081',
          region: 'us-east-1',
          accessKeyId: 'test',
          secretAccessKey: 'test',
        },
      }),
      ...config,
    })
  }

  public metaStore() {
    return new AwsMetaStore({
      tableName: `ddes-${this.testId}-meta`,
      ...(!process.env.REAL_SERVICES_TEST && {
        dynamodbClientConfiguration: {
          endpoint: process.env.LOCAL_DDB || 'http://localhost:8081',
          region: 'us-east-1',
          accessKeyId: 'test',
          secretAccessKey: 'test',
        },
      }),
    })
  }

  public snapshotStore() {
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
      bucketName: `ddes-${this.testId}`,
      keyPrefix: 'snapshots/',
      manageBucket: true,
    })
  }
}