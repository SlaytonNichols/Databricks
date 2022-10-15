-- Databricks notebook source
-- DBTITLE 1,Create Silver Schema
CREATE SCHEMA IF NOT EXISTS silver
COMMENT "Silver Schema"
LOCATION "/mnt/silver"

-- COMMAND ----------

-- DBTITLE 1,Create Gold HeartRatesCleaned Table
CREATE TABLE IF NOT EXISTS silver.heartrates_cleaned USING DELTA LOCATION '/mnt/bronze/heartrates_cleaned'

-- COMMAND ----------

-- DBTITLE 1,Create Silver Date Dimension
CREATE TABLE IF NOT EXISTS silver.date
LOCATION "/mnt/silver/date"
AS
SELECT 
  to_date(d.Date) as `date`,
  d.DateKey,
  d.DayName,
  to_number(d.DayOfMonth, "999") as `dayOfMonth`,
  to_number(d.DayOfQuarter, "999") as dayOfQuarter,
  to_number(d.DayOfWeekInMonth, "999") as dayOfWeekInMonth,
  to_number(d.DayOfWeekInYear, "999") as dayOfWeekInYear,
  to_number(d.DayOfWeekUK, "999") as dayOfWeekUk,
  to_number(d.DayOfWeekUSA, "999") as dayOfWeekUsa,
  to_number(d.DayOfYear, "999") as `dayOfYear`,
  d.DaySuffix as daySuffix,
  to_date(d.FirstDayOfMonth) as firstDayOfMonth,
  to_date(d.FirstDayOfQuarter) as firstDayOfQuarter,
  to_date(d.FirstDayOfYear) as firstDayOfYear,
  to_timestamp(d.FullDateUK) as fullDateUk,
  to_timestamp(d.FullDateUSA) as fullDateUSA,
  d.HolidayUK as holidayUk,
  d.HolidayUSA as holidayUsa,
  d.IsHolidayUK as isHolidayUk,
  d.IsHolidayUSA as isHolidayUsa,
  d.IsWeekday as isWeekDay,
  to_date(d.LastDayOfMonth) as lastDayOfMonth,
  to_date(d.LastDayOfQuarter) as lastDayOfQuarter,
  to_date(d.LastDayOfYear) as lastDayOfYear,
  bigint(d.MMYYYY) as mmyyyy,
  to_number(d.Month, "999") as `month`,
  d.MonthName as monthName,
  to_number(d.MonthOfQuarter, "999") as monthOfQuarter,
  to_number(d.Quarter, "999") as `quarter`,
  d.QuarterName as quarterName,
  to_number(d.WeekOfMonth, "999") as weekOfMonth,
  to_number(d.WeekOfQuarter, "999") as weekOfQuarter,
  to_number(d.WeekOfYear, "999") as `weekOfYear`,
  to_number(d.Year, "9999") as `year`,
  d.YearName as yearName
FROM bronze.date d

-- COMMAND ----------


