package dpr.spike.service

import java.io.PrintWriter
import java.io.StringWriter
import org.apache.spark.sql.types.DataType
import scala.collection.mutable
import scala.io.Source


object SourceReferenceService {
  private val REF = new mutable.HashMap[String, SourceReference]

    def init(): Unit = {
      // demo
      // local testing
      REF.put("public.offenders", new SourceReference("SYSTEM.OFFENDERS", "nomis", "offenders",
        "offender_id", "", getSchemaFromResource("schemas/oms_owner.offenders.schema.json")))
      REF.put("public.offender_bookings", new SourceReference("SYSTEM.OFFENDER_BOOKINGS",
        "nomis", "offender_bookings", "offender_book_id", "",
        getSchemaFromResource("schemas/oms_owner.offender_bookings.schema.json")))
    }

    def init_hudi(): Unit = {
      // demo
      // local testing
      REF.put("public.offenders", new SourceReference("SYSTEM.OFFENDERS", "nomis", "offenders",
        "offender_id", "create_date", getSchemaFromResource("schemas/oms_owner.offenders.schema.json")))
      REF.put("public.offender_bookings", new SourceReference("SYSTEM.OFFENDER_BOOKINGS",
        "nomis", "offender_bookings", "offender_book_id", "booking_begin_date",
        getSchemaFromResource("schemas/oms_owner.offender_bookings.schema.json")))
  }


    def getPrimaryKey(key: String): String = {
      val ref = REF.getOrElse(key.toLowerCase, null)
      if (ref == null) null
      else ref.getPrimaryKey
    }

    def getPartitionKey(key: String): String = {
      val ref = REF.getOrElse(key.toLowerCase, null)
      if (ref == null) null
      else ref.getPartitionKey
    }

    def getSource(key: String): String = {
      val ref = REF.getOrElse(key.toLowerCase, null)
      if (ref == null) null
      else ref.getSource
    }

    def getTable(key: String): String = {
      val ref = REF.getOrElse(key.toLowerCase, null)
      if (ref == null) null
      else ref.getTable
    }

    def getSchema(key: String): DataType = {
      println(key.toLowerCase())
      val ref = REF.getOrElse(key.toLowerCase, null)
      if (ref == null) null
      else ref.getSchema
    }

  def getCasts(key: String): Map[String, String] = {
    val ref = REF.get(key.toLowerCase)
    if (ref == null) null
    else null
  }

  //  def getReferences = new HashSet[SourceReferenceService.SourceReference](REF.values)

  private def getSchemaFromResource(resource: String): DataType = {
    println(resource)
    val json_string = Source.fromResource(resource).mkString
    DataType.fromJson(json_string)
  }


  protected def handleError(e: Exception): Unit = {
    val sw = new StringWriter()
    e.printStackTrace(new PrintWriter(sw))
    System.err.print(sw.getBuffer.toString)
  }
}

class SourceReferenceService {

}



