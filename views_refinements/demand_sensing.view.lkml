include: "/views/demand_sensing.view"
view: +demand_sensing {
# derived_table: {
   sql_table_name: (WITH
  CalDate AS (
  SELECT
    date,
    EXTRACT (week
    FROM
      date) AS week
  FROM
    UNNEST( GENERATE_DATE_ARRAY( DATE_ADD( @{use_for_current_date}, INTERVAL -CAST( 1 AS INT64) YEAR ), DATE_ADD(
    @{use_for_current_date}, INTERVAL 13 WEEK))) AS date ),
  Grid AS (
  SELECT
    DISTINCT SalesOrders.MaterialNumber_MATNR AS Product,
    SalesOrders.ShipToPartyItem_KUNNR AS Customer,
    SalesOrders.ShipToPartyItemName_KUNNR AS CustomerName,
    SalesOrders.RequestedDeliveryDate_VDATU AS Date,
    Customers.City_ORT01 AS Location,
    Customers.PostalCode_PSTLZ AS PostalCode
  FROM
    `@{GCP_PROJECT}.@{REPORTING_DATASET}.SalesOrders` SalesOrders
  LEFT JOIN
    `@{GCP_PROJECT}.@{REPORTING_DATASET}.CustomersMD` Customers
  ON
    SalesOrders.Client_MANDT=Customers.Client_MANDT
    AND SalesOrders.ShipToPartyItem_KUNNR=Customers.CustomerNumber_KUNNR
  WHERE
    SalesOrders.Client_MANDT = "@{CLIENT}"
  UNION DISTINCT
  SELECT
    DemandForecast.CatalogItemID AS Product,
    DemandForecast.CustomerId AS Customer,
    CustomersMD.Name1_NAME1 AS CustomerName,
    DemandForecast.StartDateOfWeek AS Date,
    CustomersMD.City_ORT01 AS Location,
    CustomersMD.PostalCode_PSTLZ AS PostalCode
  FROM
    `@{GCP_PROJECT}.@{REPORTING_DATASET}.DemandForecast`DemandForecast
    INNER JOIN `@{GCP_PROJECT}.@{REPORTING_DATASET}.CustomersMD` CustomersMD
  ON CustomersMD.CustomerNumber_KUNNR=DemandForecast.CustomerId
and CustomersMD.Client_MANDT= "@{CLIENT}" ),
  Sales AS (
  SELECT
    SalesOrders.Client_MANDT AS Client_MANDT,
    SalesOrders.MaterialNumber_MATNR AS Product,
    SalesOrders.RequestedDeliveryDate_VDATU AS Date,
    SalesOrders.ShipToPartyItem_KUNNR AS Customer,
    SalesOrders.ShipToPartyItemName_KUNNR AS CustomerName,
    SalesOrders.CumulativeOrderQuantity_KWMENG AS SalesOrderQuantity,
    Customers.City_ORT01 AS Location,
    Customers.CountryKey_LAND1 AS Country,
    Customers.PostalCode_PSTLZ AS PostalCode,
    SUM(SalesOrders.CumulativeOrderQuantity_KWMENG) OVER(PARTITION BY SalesOrders.MaterialNumber_MATNR ORDER BY SalesOrders.RequestedDeliveryDate_VDATU ASC ROWS 13 PRECEDING) AS Past13WeekSales,
    SUM(SalesOrders.CumulativeOrderQuantity_KWMENG) OVER(PARTITION BY SalesOrders.MaterialNumber_MATNR ORDER BY SalesOrders.RequestedDeliveryDate_VDATU ASC ROWS 52 PRECEDING) AS Past52WeekSales,
  FROM
    `@{GCP_PROJECT}.@{REPORTING_DATASET}.SalesOrders` SalesOrders
  LEFT JOIN
    `@{GCP_PROJECT}.@{REPORTING_DATASET}.CustomersMD` Customers
  ON
    SalesOrders.Client_MANDT=Customers.Client_MANDT
    AND SalesOrders.ShipToPartyItem_KUNNR=Customers.CustomerNumber_KUNNR
  WHERE
    SalesOrders.Client_MANDT= "@{CLIENT}" ),
  Forecast AS (
  SELECT
    DemandForecast.CatalogItemID AS Product,
    DemandForecast.StartDateOfWeek AS Date,
    DemandForecast.CustomerId AS Customer,
    DemandForecast.ForecastQuantity AS Sales,
    CustomersMD.City_ORT01 AS Location,
    DemandForecast.ForecastQuantityLowerBound,
    DemandForecast.ForecastQuantityUpperBound
  FROM
    `@{GCP_PROJECT}.@{REPORTING_DATASET}.DemandForecast` DemandForecast
    INNER JOIN `@{GCP_PROJECT}.@{REPORTING_DATASET}.CustomersMD` CustomersMD
  ON CustomersMD.CustomerNumber_KUNNR=DemandForecast.CustomerId
and CustomersMD.Client_MANDT= "@{CLIENT}"),

  DemandPlan AS (
  SELECT
    DemandPlan.MaterialNumber AS Product,
    DemandPlan.WeekStart AS Date,
    DemandPlan.CustomerId AS Customer,
    DemandPlan.DemandPlan AS Sales,
    CustomersMd.City_ORT01 AS Location,
  FROM
    `@{GCP_PROJECT}.@{REPORTING_DATASET}.DemandPlan` DemandPlan

INNER JOIN `@{GCP_PROJECT}.@{REPORTING_DATASET}.CustomersMD` CustomersMD

ON CustomersMD.CustomerNumber_KUNNR=DemandPlan.CustomerId
and CustomersMD.Client_MANDT= "@{CLIENT}" ),

  Weather AS(
SELECT
Weather.MaxTemp,
Weather.MinTemp,
Weather.Country,
Weather.PostCode,
Weather.Date,
Weather.AvgMaxTemp,
Weather.AvgMinTemp,
IF
    (COUNT(CASE
          WHEN (MaxTemp>AvgMaxTemp) THEN 1
      END
        ) OVER (PARTITION BY EXTRACT(year FROM Weather.Date),
        EXTRACT(week
        FROM
          Weather.Date),
        Weather.PostCode)>=2,
      TRUE,
      FALSE) AS HeatWave,
  IF
    (COUNT(CASE
          WHEN (MinTemp<AvgMinTemp) THEN 1
      END
        ) OVER (PARTITION BY EXTRACT(year FROM Weather.Date),
        EXTRACT(week
        FROM
          Weather.Date),
        Weather.PostCode)>=2,
      TRUE,
      FALSE) AS ColdFront
From
(SELECT
  MaxTemp  MaxTemp,
  MinTemp  MinTemp,
  Country Country,
  PostCode PostCode,
  date Date,
  AVG(MaxTemp) OVER(PARTITION BY postcode, extract (week from date) order by extract (week from date) RANGE BETWEEN 20 PRECEDING AND CURRENT ROW) AvgMaxTemp,
  AVG(MinTemp) OVER(PARTITION BY postcode, extract (week from date) order by extract (week from date) RANGE BETWEEN 20 PRECEDING AND CURRENT ROW) AvgMinTemp,
FROM
   @{GCP_PROJECT}.@{REPORTING_DATASET}.WeatherDaily) As Weather),
  Trends AS(
  SELECT
  WeekStart,
  Week,
  InterestOverTime,
  Country,
  HierarchyId,
  SearchTerm,
  HistoricalMin,
  HistoricalMax,
  ((InterestOverTime-HistoricalMin)/(HistoricalMax-HistoricalMin))*100 AS NormalizedScore,
  AVG(((InterestOverTime-HistoricalMin)/(HistoricalMax-HistoricalMin))*100) OVER (PARTITION BY Country, HierarchyId )AS AvgNormalizedScore,
  AVG(((InterestOverTime-HistoricalMin)/(HistoricalMax-HistoricalMin))*100) OVER (PARTITION BY Country, HierarchyId ORDER BY Trends.Week RANGE BETWEEN 44 PRECEDING AND CURRENT ROW )AS AvgNormalizedScoreFor10Months,
FROM (
  SELECT
    WeekStart,
    Extract( WEEK from WeekStart) as Week,
    InterestOverTime,
    CountryCode as Country,
    HierarchyId,
    HierarchyText as SearchTerm,
    MIN(InterestOverTime) OVER (PARTITION BY CountryCode, HierarchyId, EXTRACT(WEEK FROM CAST(WeekStart AS date)) ) AS HistoricalMin,
    MAX(InterestOverTime) OVER (PARTITION BY CountryCode, HierarchyId, EXTRACT(WEEK FROM CAST(WeekStart AS date)) ) AS HistoricalMax
  FROM
    `@{GCP_PROJECT}.@{REPORTING_DATASET}.Trends`)Trends
WHERE
  HistoricalMin != HistoricalMax ),
  Materials as (Select
    MaterialsMD.MaterialText_MAKTX,
    MaterialsMD.MaterialNumber_MATNR,
    MaterialsMD.Client_MANDT,
    ProductHierarchyText.Description_vtext as HierarchyText
    from
    `@{GCP_PROJECT}.@{REPORTING_DATASET}.MaterialsMD` MaterialsMD
    left JOIN
(SELECT
    distinct Hierarchy_Prodh,Client_MANDT,Description_vtext,Level_STUFE,Language_SPRAS
  FROM
   `@{GCP_PROJECT}.@{REPORTING_DATASET}.ProductHierarchiesMD`) ProductHierarchyText
  ON left(MaterialsMD.ProductHierarchy_Prdha, 6 ) = ProductHierarchyText.Hierarchy_Prodh
    AND MaterialsMD.Client_MANDT = ProductHierarchyText.Client_MANDT
    AND ProductHierarchyText.Level_STUFE='3'
    AND ProductHierarchyText.Language_SPRAS='E'
  )
SELECT
  CalDate.Date,
  CalDate.Week,
  Grid.Product,
  Sales.Client_MANDT,
  materials.MaterialText_MAKTX AS ProductName,
  Grid.CustomerName AS Customer,
  Grid.Location,
  Sales.Country,
  Sales.SalesOrderQuantity AS Sales,
  Sales.Past13WeekSales,
  Sales.Past52WeekSales,
  DemandPlan.Sales DemandPlan,
  ROUND(Forecast.Sales,1) ForecastQuantity,
  ROUND(Forecast.ForecastQuantityLowerBound, 1) AS ForecastQuantityLowerBound,
  ROUND(Forecast.ForecastQuantityUpperBound, 1) AS ForecastQuantityUpperBound,
  Trends.SearchTerm,
  Trends.InterestOverTime,
  ROUND(Weather.MaxTemp,1) AS AverageHighTemperature,
  ROUND(Weather.MinTemp,1) AS AverageLowTemperature,
  ROUND((Weather.MaxTemp+Weather.MinTemp)/2,1) AverageTemperature,
  ROUND(AVG((Weather.MaxTemp+Weather.MinTemp)/2) OVER (PARTITION BY Grid.Location, Grid.Product ORDER BY CalDate.Date ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING ),1) AS MovingAverageTemperature,
  PromotionCalendar.DiscountPercent,
  PromotionCalendar.IsPromo,
  IF(HolidayCalendar.HolidayDate IS NULL,0,1) AS IsHoliday,
  ---ForecastOutsideStatisticalRange Impact Score
IF
  ((DemandPlan.Sales > Forecast.ForecastQuantityUpperBound)
    OR (DemandPlan.Sales < Forecast.ForecastQuantityLowerBound),
    ROUND((ABS(Forecast.Sales-DemandPlan.Sales)/DemandPlan.Sales)*100,2),
    0) AS ForecastOutsideStatisticalRangeImpactScore,
---HeatWave Impact Score
IF(HeatWave IS TRUE,
    IF(CorrValue>0.0
    AND (LAG(DemandPlan.sales,1,0)OVER(PARTITION BY Sales.product, Sales.location, EXTRACT(week FROM CalDate.date)
      ORDER BY EXTRACT(week FROM CalDate.date)ASC))>Forecast.ForecastQuantityUpperBound-0.05*Forecast.ForecastQuantityUpperBound,
    ROUND((ABS(Forecast.Sales-DemandPlan.Sales)/DemandPlan.Sales)*100,2),
    IF(CorrValue<0.0
    And (LAG(DemandPlan.sales,1,0)OVER(PARTITION BY EXTRACT(week FROM CalDate.date)
      ORDER BY EXTRACT(week FROM CalDate.date)ASC))<Forecast.ForecastQuantityLowerBound+0.05*Forecast.ForecastQuantityLowerBound,
    ROUND((ABS(Forecast.Sales-DemandPlan.Sales)/DemandPlan.Sales)*100,2),0)),0) AS HeatWaveImpactScore,

---ColdFront Impact Score
IF(ColdFront IS TRUE,
    IF(CorrValue<0.0
    AND (LAG(DemandPlan.sales,1,0)OVER(PARTITION BY Sales.product, Sales.location, EXTRACT(week FROM CalDate.date)
      ORDER BY EXTRACT(week FROM CalDate.date)ASC))>Forecast.ForecastQuantityUpperBound-0.05*Forecast.ForecastQuantityUpperBound,
    ROUND((ABS(Forecast.Sales-DemandPlan.Sales)/DemandPlan.Sales)*100,2),
    IF(CorrValue>0.0
    And (LAG(DemandPlan.sales,1,0)OVER(PARTITION BY EXTRACT(week FROM CalDate.date)
      ORDER BY EXTRACT(week FROM CalDate.date)ASC))<Forecast.ForecastQuantityLowerBound+0.05*Forecast.ForecastQuantityLowerBound,
    ROUND((ABS(Forecast.Sales-DemandPlan.Sales)/DemandPlan.Sales)*100,2),0)),0) AS ColdFrontImpactScore,

  --- PromoDiffrential Impact Score
IF
  ( PromotionCalendar.IsPromo=true
    AND (DemandPlan.Sales>Forecast.ForecastQuantityUpperBound
      OR DemandPlan.Sales<Forecast.ForecastQuantityLowerBound),
    ROUND((ABS(Forecast.Sales-DemandPlan.Sales)/DemandPlan.Sales)*100,2),
    0) AS PromoDiffrentialImpactScore,
  ---NonSeasonalTrends Impact Score
IF
  ((DemandPlan.Sales>Forecast.ForecastQuantityUpperBound
      OR DemandPlan.Sales<Forecast.ForecastQuantityLowerBound)
    AND Trends.Week BETWEEN Trends.Week
    AND Trends.Week+3
    AND ((ABS(NormalizedScore-AvgNormalizedScoreFor10Months)/ AvgNormalizedScoreFor10Months)*100 > 0.25),
    ROUND((ABS(Forecast.Sales-DemandPlan.Sales)/DemandPlan.Sales)*100,2),
    0) AS NonSeasonalTrendsImpactScore,

FROM
  CalDate
LEFT JOIN
  Grid
ON
  CalDate.date = Grid.date
LEFT JOIN
  Sales
ON
  CalDate.date = Sales.date
  AND Grid.product = Sales.product
  AND Grid.customer = Sales.customer
  AND Grid.location = Sales.location
LEFT JOIN
  Forecast
ON
  CalDate.date = Forecast.date
  AND Grid.product = Forecast.product
  AND Grid.customer = Forecast.customer
  AND Grid.location = Forecast.location
LEFT JOIN
  DemandPlan
ON
  CalDate.date = DemandPlan.date
  AND Grid.product = DemandPlan.product
  AND Grid.customer = DemandPlan.customer
  AND Grid.location = DemandPlan.location
LEFT JOIN
  Weather
ON
  Grid.PostalCode=Weather.PostCode
  AND CalDate.Date=Weather.Date
LEFT JOIN
  ${correlation_table_pdt.SQL_TABLE_NAME} AS CorrelationTable
ON
  Grid.Location=CorrelationTable.Location
  AND Grid.Product=CorrelationTable.product
LEFT JOIN
  Materials
ON
  Grid.product =Materials.MaterialNumber_MATNR
  AND Materials.Client_MANDT= "@{CLIENT}"
  --AND Sales.Client_MANDT=Materials.Client_MANDT
LEFT JOIN
  Trends
ON
  CalDate.date=Trends.WeekStart
  AND Materials.HierarchyText = Trends.SearchTerm
  AND Sales.Country=Trends.Country
LEFT JOIN
  `@{GCP_PROJECT}.@{REPORTING_DATASET}.PromotionCalendar` PromotionCalendar
ON
  CalDate.date=PromotionCalendar.StartDateOfWeek
  AND Grid.product=PromotionCalendar.CatalogItemId
  AND Grid.customer=PromotionCalendar.Customerid
LEFT JOIN
  `@{GCP_PROJECT}.@{REPORTING_DATASET}.HolidayCalendar` HolidayCalendar
ON
  Grid.Date = HolidayCalendar.HolidayDate) ;;




  dimension: moving_average {
    type: number
    sql:Case
        When CAST(${TABLE}.Date AS DATE) <= CAST (@{use_for_current_date} AS DATE)
        THEN ${TABLE}.MovingAverageTemperature
        End;;
  }
  measure: temperature {
    type: average
    sql:
      CASE
        WHEN CAST(${TABLE}.Date AS DATE) <= CAST (@{use_for_current_date} AS DATE)
        THEN ${average_temperature}
        ELSE NULL
      END ;;
  }

  measure: forecast_temperature {
    type: average
    sql:
      CASE
        WHEN CAST(${TABLE}.Date AS DATE) > CAST (@{use_for_current_date} AS DATE)
        THEN ${average_temperature}
        ELSE NULL
      END ;;
  }

  dimension: demand_plan {
    type: number
    sql:
    CASE
      WHEN CAST(${TABLE}.Date AS DATE) > CAST (@{use_for_current_date} AS DATE)
      THEN ${TABLE}.DemandPlan
    ELSE
      NULL
    END;;
  }

  measure: thirteen_week_sale{
    type: sum
    sql:
    CASE
      WHEN CAST(${TABLE}.Date AS DATE) <= CAST (@{use_for_current_date} AS DATE) and DATE_DIFF(CAST (@{use_for_current_date} AS DATE), Cast(${TABLE}.Date as Date), Day)<=91
      THEN ${TABLE}.Sales
    ELSE
      NULL
    END;;
  }

  measure: fifty_two_week_sale{
    type: sum
    sql:
    CASE
      WHEN ${TABLE}.RecordType='SalesOrders' and CAST(${TABLE}.Date AS DATE) <= CAST (@{use_for_current_date} AS DATE) and DATE_DIFF(CAST (@{use_for_current_date} AS DATE), Cast(${TABLE}.Date as Date), Day)<=366
      THEN ${TABLE}.Sales
    ELSE
      NULL
    END;;
  }

  measure: wholesale_quantity_measure {
    type: average
    sql:
     CASE
      WHEN CAST(${TABLE}.Date AS DATE) <= CAST (@{use_for_current_date} AS DATE)
      THEN round(${order_quantity})
     ELSE
      NULL
    END;;
  }

  measure: forecast {
    type: average
    sql:
    CASE
      WHEN CAST(${TABLE}.Date AS DATE) > CAST (@{use_for_current_date} AS DATE)
      THEN round(${forecast_quantity})
    ELSE
      NULL
    END;;
  }

  measure: forecast_lower {
    type: average
    sql:
    CASE
     WHEN CAST(${TABLE}.Date AS DATE) > CAST (@{use_for_current_date} AS DATE)
     THEN round(${forecast_quantity_lower_bound})
    ELSE
     NULL
    END;;
  }
  measure: forecast_upper {
    type: average
    sql:
    CASE
     WHEN CAST(${TABLE}.Date AS DATE) > CAST (@{use_for_current_date} AS DATE)
     THEN round(${forecast_quantity_upper_bound})
    ELSE
     NULL
    END;;
  }

  measure: thirteen_week_forecast{
    type: sum
    sql:
      CASE
        WHEN CAST(${TABLE}.Date AS DATE) > CAST (@{use_for_current_date} AS DATE) and DATE_DIFF(Cast(${TABLE}.Date as Date), CAST (@{use_for_current_date} AS DATE), Day)<=91
        THEN ${forecast_quantity}
      END ;;
    value_format_name: decimal_0
  }

 }
