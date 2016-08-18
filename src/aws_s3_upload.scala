package com.common.log

import java.io.ByteArrayInputStream
import java.io.File
import java.io.InputStream
import java.util.List

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.Bucket
import com.amazonaws.services.s3.model.CannedAccessControlList
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.amazonaws.services.s3.model.ListObjectsRequest
import java.util.Date
import com.amazonaws.services.s3.model.AmazonS3Exception
import com.typesafe.config.{Config, ConfigFactory}
//remove if not needed
import scala.collection.JavaConversions._

class AwsS3Appender {

  private val SUFFIX = "/"
  private val conf: Config = ConfigFactory.load
  private val logging: Config = conf.getConfig("logging")
  private val AWS_S3_ACCESS_ID: String = logging.getString("AWS_S3_ACCESS_ID")
  private val AWS_S3_SECURITY_KEY: String = logging.getString("AWS_S3_SECURITY_KEY")
  private val AWS_S3_BUCKET_NAME: String = logging.getString("AWS_S3_BUCKET_NAME")
  private val AWS_S3_LOCAL_LOG_PATH: String = logging.getString("AWS_S3_LOCAL_LOG_PATH")
  private val AWS_S3_UPLOAD_FOLDER_NAME: String = logging.getString("AWS_S3_UPLOAD_FOLDER_NAME")


  def sendLogToAwsS3() {
    try {
      val credentials = new BasicAWSCredentials(AWS_S3_ACCESS_ID, AWS_S3_SECURITY_KEY)
      val s3client = new AmazonS3Client(credentials)
      val bucketName = AWS_S3_BUCKET_NAME
      val uploadFolderName = AWS_S3_UPLOAD_FOLDER_NAME

      val logPath = new File(AWS_S3_LOCAL_LOG_PATH)
      val paths = logPath.listFiles()
      val date = new Date()
      val extension = "log"

      // check existing (same names) directory file on S3 path if it is not, create new directory file. 
      if (!isExistS3(s3client, bucketName, uploadFolderName)) createFolder(bucketName, uploadFolderName, s3client)

      //read all files list on the path
      for (path <- paths) {

        //println(path.getName)
        //get files name
        val fileName = path.getName

        //check file name ( .log ) and upload files to S3
        if (fileName.toLowerCase().endsWith(extension)) {
          val uploadFileName = uploadFolderName + SUFFIX + fileName

          //upload
          s3client.putObject(new PutObjectRequest(bucketName, uploadFileName, new File(path.toString))
            .withCannedAcl(CannedAccessControlList.PublicRead))

          //delete files on local after copying to S3
          path.deleteOnExit()
        }
      }
    } catch {

      case e: AmazonS3Exception =>
        Logger.getLogger(getClass()).error("Fail to send S3 : " + e.toString);
      case _ =>
        Logger.getLogger(getClass()).debug("Fail to send S3");
    }
  }

  def createFolder(bucketName: String, folderName: String, client: AmazonS3) {
    val metadata = new ObjectMetadata()
    metadata.setContentLength(0)
    val emptyContent = new ByteArrayInputStream(Array.ofDim[Byte](0))
    val putObjectRequest = new PutObjectRequest(bucketName, folderName + SUFFIX, emptyContent, metadata)
    client.putObject(putObjectRequest)
  }

  def isExistS3(s3Client: AmazonS3Client, bucketName : String, fileName : String) : Boolean = {

    val objects = s3Client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(fileName))

    for (objectSummary <- objects.getObjectSummaries()) {
      if (objectSummary.getKey().equals(fileName)) {
        return true
      }
    }
    return false
  }
}
