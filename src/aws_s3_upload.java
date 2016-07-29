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

      // 기존에 만들어진 폴더가 있는지 체크하고 없으면 새로 생성한다. (최초 1회 실행 됨
      if (!isExistS3(s3client, bucketName, uploadFolderName)) createFolder(bucketName, uploadFolderName, s3client)

      //해당 폴더에 생성되어 있는 모든 로그들의 리스트 (파일 경로 + 파일이름)를 받아온다.
      for (path <- paths) {

        //println(path.getName)
        //로그 파일들의 이름을 얻어온다.
        val fileName = path.getName

        //해당 파일이 .log 로 끝나는지 확인한다.
        //.log 가 아닌 파일은 제외한다.
        if (fileName.toLowerCase().endsWith(extension)) {
          val uploadFileName = uploadFolderName + SUFFIX + fileName

          //해당 파일을 업로드 한다.
          s3client.putObject(new PutObjectRequest(bucketName, uploadFileName, new File(path.toString))
            .withCannedAcl(CannedAccessControlList.PublicRead))

          //업로드 된 파일은 로컬에서 삭제한다.
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
