package io.peerdb.flow.jvm.iceberg.catalog.io.mapper;

import org.apache.iceberg.aws.s3.S3FileIO;

/**
 * TODO This class should act as a delegate to the S3FileIO class, temporarily fixing <a href="https://github.com/apache/iceberg/issues/9785"> </a> till it is fixed upstream.
 * Maybe a way to use it is to use Lombok's @Delegate functionality.
 */
public class FixedS3FileIO extends S3FileIO {
}
