
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{lit, when, datediff, month, dayofmonth, last_day, round}

val dayFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
val rangeBegin = LocalDate.parse("2020-01-01", dayFormat)
val tmp = ""
//if (tmp == "") {
  val rangeEnd = rangeBegin.plusYears(1).minusDays(1)
//} else {
//  val rangeEnd = LocalDate.parse(tmp, dayFormat)
  //val rangeEnd = if (options.exists(_._1 == "rangeEnd")) LocalDate.parse(options("rangeEnd"), dayFormat) else rangeStart.plusYears(1).minusDays(1)
//}

val preciseMonth = false
/**
 * Function calculates the intersection of 2 date ranges in months (float).
 * E.g. a customer has a membership from 12.05.2015 - 31.03.2020 and the target range is the year 2020,
 * results in 3.0 month.
 * @param startDate given start date, e.g customer membership start date
 * @param endDate given end date, e.g customer membership end date
 * @param rangeBegin reference start date
 * @param rangeEnd reference end date
 * @return month range
 */
def datesToMonths(startDate: Column, endDate: Column, rangeBegin: LocalDate, rangeEnd: LocalDate): Column = {
  val diff = datediff(lit(rangeEnd), startDate)
  // double check if there is a period within the specified range
  val colBegin = when(datediff(lit(rangeEnd), startDate) <= 0, null)
    .when(datediff(startDate, lit(rangeBegin)) <=0, rangeBegin)
    .otherwise(startDate)
  val colEnd = when(datediff(endDate, lit(rangeBegin)) <= 0, null)
    .when(datediff(lit(rangeEnd), endDate) <=0, rangeEnd)
    .otherwise(endDate)
  // compute the periode
  // -- TODO decide if computed with constant 30: m+d/30 or accurate
  val monthBeg = month(colBegin)-1 + (dayofmonth(colBegin) -1)/ dayofmonth(last_day(colBegin))
  val monthEnd = month(colEnd)-1 + dayofmonth(colEnd) / dayofmonth(last_day(colEnd))
  round( monthEnd - monthBeg, 2 )
  monthEnd - monthBeg
}
/**
 *
 * @param df_familie
 * @param df_gemeinde
 * @param rangeBegin
 * @param rangeEnd
 */
def getFamKantons(df_familie: DataFrame, df_gemeinde: DataFrame, rangeBegin: LocalDate, rangeEnd: LocalDate) = {
//   df_familie
//        .where($"fam_end_dat" >= rangeBegin && $"fam_beg_dat" <= rangeEnd)
//        .join(df_gemeinde.select("gde_id", "gde_kt"), $"fam_ra_gde_id" === $"gde_id", join_type).drop("fam_ra_gde_id")
//        .withColumnRenamed("gde_kt", "Kanton")
//        // TODO: merge rows with subsequent periods in same canton
//        // remove periods military
}

(session: SparkSession, options: Map[String,String], dfs: Map[String,DataFrame]) => {
  import session.implicits._
 
  assert(options.exists(_._1 == "rangeStart"), s"rangeStart needs to be defined, with format YYYY-MM-DD")
  //assert(options.exists(_._1 == "rangeEnd"), s"rangeEnd needs to be defined, with format YYYY-MM-DD")

  val dayFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val rangeStart = LocalDate.parse(options("rangeStart"), dayFormat)
  //try {
  //    rangeStart = LocalDate.parse(options("rangeStart"), dayFormat)
  //  } catch {
  //    case _: Throwable => println(" cannot convert rangeStart to date. Please specify the date in format YYYY-MM-DD")
  //  }
  //}
  val rangeEnd = if (options.exists(_._1 == "rangeEnd")) LocalDate.parse(options("rangeEnd"), dayFormat) else rangeStart.plusYears(1).minusDays(1)

  val df_leiPPos = dfs("stg-LeiPPos")
  val df_leiPKopf = dfs("stg-LeiPKopf")
  val df_vers = dfs("stg-Versicherter")
  val df_familie = dfs("stg-Familie")
  val df_gemeinde = dfs("stg-Gemeinde")
  val df_ecKopf = dfs("stg-ECKopf")
  val df_ecDetail = dfs("stg-ECDetail")
  val df_ecProdukt = dfs("stg-ECProdukt")

  val join_type = "left" // "left_semi" would only take rows which match

  val df_familieWithMonths = df_familie.withColumn("fam_versmon", datesToMonths($"fam_beg_dat", $"fam_end_dat", rangeStart, rangeEnd))

  val df = df_leiPPos
      // filter for specified time range
      .filter($"LPP_BEH_BEG_DAT" >= rangeStart && $"LPP_BEH_BEG_DAT" <= rangeEnd )

      // BAG-Nummer
      //TODO
      .withColumn("BAG-Nummer", lit("TBD"))

      // Jahr
      //TODO
      .withColumn("Jahr", lit("TBD"))


      // AHV-Nr: LeiPPos.LPP_LPK_ID → LeiPKopf.LPK_VER_NR → Versicherter.VER_NNSS_NR
      .join(df_leiPKopf, $"lpp_lpk_id" === $"lpk_id", join_type)
      .join(df_vers, $"lpk_ver_nr" === $"ver_nr", join_type)
      .withColumnRenamed("ver_nnss_nr", "AHV-Nummer")

      // Wohnkanton: LeiPPos.LPP_LPK_ID → LeiPKopf.LPK_VER_NR → Versicherter.VER_FAM_NR → Familie.FAM_RA_GDE_ID (Test auf Zeitraum) → Gemeinde.GDE_KT
      .join(df_familieWithMonths, $"ver_fam_nr" === $"fam_nr", join_type)
      .join(df_gemeinde, $"fam_ra_gde_id" === $"gde_id", join_type)
      .withColumnRenamed("gde_kt", "Kanton")

      // Geburtsjahr: LeiPPos.LPP_LPK_ID → LeiPKopf.LPK_VER_NR → Versicherter.VER_GEB_DAT
      .withColumnRenamed("ver_geb_dat", "Geburtsjahr")

      // Geschlecht: LeiPPos.LPP_LPK_ID → LeiPKopf.LPK_VER_NR → Versicherter.VER_SEX_CD
      .withColumnRenamed("ver_sex_cd", "Geschlecht")

      // Spital Aufenthalt:
      // Exakte Ein/Austritte Spital/Reha o.ä. ECP: LeiPPos.LPP_LPK_ID → LeiPKopf.LPK_RECH_NR → ECKopf.ECK_RECH_NR, ~.ECK_ID → ECKopfXtraCaseDetail.EXC_ECK_ID, ~.EXC_BEG_DAT, ~.EXC_END_DAT (korrekte Summierung)
      // Exakte Ein/Austritte Spital/Reha o.ä. Sumex/Secon: LeiPPos.LPP_LPK_ID → LpkCaseDetail.LCD_BEG_DAT,~.LCD_END_DAT 
      // TODO
      .withColumn("Aufenthalt", lit("TBD"))

      // GTIN: LeiPPos.LPP_LPK_ID → LeiPKopf.LPK_RECH_NR → ECKopf.ECK_RECH_NR, ~.ECK_ID → ECDetail.ECD_ECP_ID→ ECProdukt.ECP_PRODUKT_NR (PharmaCode)
      // 13-stellig
      //TODO double check
      .join(df_ecKopf, $"LPK_RECH_NR" === $"ECK_RECH_NR", join_type)
      .join(df_ecDetail, $"ECK_ID" === $"ECD_ECK_ID", join_type)
      .join(df_ecProdukt, $"ECD_ECP_ID" === $"ECP_ID", join_type)
      .withColumnRenamed("ecp_produkt_nr", "GTIN")

      // PharmaCode (5-7 stellig)
      //TODO
      .withColumn("PharmaCode", lit("TBD"))

      // Packungen - Da die Leistungserbringer nicht immer ganze Packungen abgeben (z.B. Abgabe einzelner Tabletten),
        // ist die Packungsanzahl mit zwei Kommastellen anzugeben (kaufmännisch gerundet).
      //TODO
      .withColumn("Packungen", lit("TBD"))

      // Monate - Die angebrochenen Versicherungsmonate bei Beginn und Ende der Versicherungsdeckung sind
        // in der Datenlieferung taggenau zu berücksichtigen (siehe Kapitel 3.7.1).
        // Die Versicherungsmonate sind deshalb mit zwei Kommastellen anzugeben (kaufmännisch gerundet).
      .withColumnRenamed("fam_versmon", "Monate")

      // Kosten - Angegeben werden müssen die Kosten vor Abzug der Kostenbeteiligung
        //(d.h. die Bruttokosten). Jeweils in Franken mit zwei Kommastellen anzugeben (kaufmännisch gerundet).
      //TODO
      .withColumn("Kosten", lit("TBD"))

      // Kostenbeteiligung - Jeweils in Franken mit zwei Kommastellen anzugeben (kaufmännisch gerundet).
      //TODO
      .withColumn("Kostenbeteiligung", lit("TBD"))

      //Output: BAG-Nummer, Jahr, AHV-Nummer, Kanton, Geburtsjahr, Geschlecht, Aufenthalt, GTIN, PharmaCode, Packungen, Monate, Kosten, Kostenbeteiligung
      .select("BAG-Nummer", "Jahr", "AHV-Nummer", "Kanton", "Geburtsjahr", "Geschlecht", "Aufenthalt", "GTIN", "PharmaCode", "Monate", "Kosten", "Kostenbeteiligung")


  Map("btl-RA-data" -> df)
}
