// sensitive keys
const omitKeys = [
  'privateKey', // snowflake and bigquery
  'password', // postgres, snowflake
  'secretAccessKey', // s3/gcs
  'subscriptionId', // eventhub
  'privateKeyId', // bigquery
  'type', // peer type
];

export default omitKeys;
